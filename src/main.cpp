// https://github.com/microsoft/vscode-cpptools/issues/9692
#if __INTELLISENSE__
#undef __ARM_NEON
#undef __ARM_NEON__
#endif

#include <Eigen/Core>

#include <pybind11/eigen.h>
#include <pybind11/iostream.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include <optional>
#include <algorithm>
#include <random>

#include <mapbox/geojson_impl.hpp>
#include <mapbox/geojson_value_impl.hpp>

#include "rapidjson/error/en.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/filewritestream.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"

#include "h3api.h"
#include "cubao/polyline_ruler.hpp"
#include "spdlog/spdlog.h"
// fix exposed macro 'GetObject' from wingdi.h (included by spdlog.h) under
// windows, see https://github.com/Tencent/rapidjson/issues/1448
#ifdef GetObject
#undef GetObject
#endif

#include <unordered_map>
#include <set>

#define STRINGIFY(x) #x
#define MACRO_STRINGIFY(x) STRINGIFY(x)

namespace py = pybind11;
using namespace pybind11::literals;

namespace cubao
{
using RapidjsonValue = mapbox::geojson::rapidjson_value;
using RapidjsonAllocator = mapbox::geojson::rapidjson_allocator;
using RapidjsonDocument = mapbox::geojson::rapidjson_document;

constexpr const auto RJFLAGS = rapidjson::kParseDefaultFlags |      //
                               rapidjson::kParseCommentsFlag |      //
                               rapidjson::kParseFullPrecisionFlag | //
                               rapidjson::kParseTrailingCommasFlag;

inline RapidjsonValue load_json(FILE *fp)
{
    char readBuffer[65536];
    rapidjson::FileReadStream is(fp, readBuffer, sizeof(readBuffer));
    RapidjsonDocument d;
    d.ParseStream<RJFLAGS>(is);
    fclose(fp);
    return RapidjsonValue{std::move(d.Move())};
}
inline RapidjsonValue load_json(const std::string &path)
{
    FILE *fp = fopen(path.c_str(), "rb");
    if (!fp) {
        return {};
    }
    return load_json(fp);
}

inline void sort_keys_inplace(RapidjsonValue &json)
{
    if (json.IsArray()) {
        for (auto &e : json.GetArray()) {
            sort_keys_inplace(e);
        }
    } else if (json.IsObject()) {
        auto obj = json.GetObject();
        // https://rapidjson.docsforge.com/master/sortkeys.cpp/
        std::sort(obj.MemberBegin(), obj.MemberEnd(), [](auto &lhs, auto &rhs) {
            return strcmp(lhs.name.GetString(), rhs.name.GetString()) < 0;
        });
        for (auto &kv : obj) {
            sort_keys_inplace(kv.value);
        }
    }
}

bool dump_json(FILE *fp, const RapidjsonValue &json, bool indent = false)
{
    using namespace rapidjson;
    char writeBuffer[65536];
    FileWriteStream os(fp, writeBuffer, sizeof(writeBuffer));
    if (indent) {
        PrettyWriter<FileWriteStream> writer(os);
        json.Accept(writer);
    } else {
        Writer<FileWriteStream> writer(os);
        json.Accept(writer);
    }
    fclose(fp);
    return true;
}

inline bool dump_json(const std::string &path, const RapidjsonValue &json,
                      bool indent)
{
    FILE *fp = fopen(path.c_str(), "wb");
    if (!fp) {
        return false;
    }
    return dump_json(fp, json, indent);
}

// https://cesium.com/learn/cesiumjs/ref-doc/HeadingPitchRoll.html
std::optional<Eigen::Vector3d>
heading_pitch_roll(const mapbox::geojson::value &extrinsic)
{
    if (!extrinsic.is<mapbox::geojson::value::object_type>()) {
        return {};
    }
    auto &obj = extrinsic.get<mapbox::geojson::value::object_type>();
    auto q = obj.find("Rwc_quat_wxyz");
    if (q == obj.end()) {
        return {};
    }
    auto p = obj.find("center");
    if (p == obj.end()) {
        return {};
    }
    auto &lla = p->second.get<mapbox::geojson::value::array_type>();
    auto &wxyz = q->second.get<mapbox::geojson::value::array_type>();
    Eigen::Matrix3d R_enu_local =
        R_ecef_enu(lla[0].get<double>(), lla[1].get<double>()).transpose() *
        Eigen::Quaterniond(wxyz[0].get<double>(), wxyz[1].get<double>(),
                           wxyz[2].get<double>(), wxyz[3].get<double>())
            .toRotationMatrix();
    Eigen::Vector3d hpr = R_enu_local.eulerAngles(2, 1, 0);
    hpr[0] *= -1.0;
    hpr[1] *= -1.0;
    hpr *= 180.0 / M_PI;
    return Eigen::round(hpr.array() * 100.0) / 100.0;
}

bool setup_extrinsic_to_heading_pitch_roll(
    const mapbox::geojson::prop_map &properties, //
    RapidjsonValue &output,                      //
    RapidjsonAllocator &allocator)
{
    auto extrinsic_itr = properties.find("extrinsic");
    if (extrinsic_itr == properties.end()) {
        return false;
    }
    auto hpr = heading_pitch_roll(extrinsic_itr->second);
    if (!hpr) {
        return false;
    }
    output.AddMember("heading", RapidjsonValue((*hpr)[0]), allocator);
    output.AddMember("pitch", RapidjsonValue((*hpr)[1]), allocator);
    output.AddMember("roll", RapidjsonValue((*hpr)[2]), allocator);
    return true;
}

struct CondenseOptions
{
    double douglas_epsilon = 0.4; // meters
    int grid_h3_resolution = 8; // https://h3geo.org/docs/core-library/restable/
    bool indent = false;
    bool sort_keys = false;
    bool grid_features_keep_properties = false;

    // https://wolf-h3-viewer.glitch.me/
    int sparsify_h3_resolution = 11;
    int sparsify_upper_limit = 42;
    bool debug = false;
};

inline void index_geometry(int index, const mapbox::geojson::geometry &geom,
                           std::vector<Eigen::Vector3d> &positions,
                           std::map<int, RowVectors> &polylines)
{
    geom.match(
        [&](const mapbox::geojson::line_string &ls) {
            RowVectors src =
                Eigen::Map<const RowVectors>(&ls[0].x, ls.size(), 3);
            auto llas = douglas_simplify(src, 1.0, true);
            positions.push_back(llas.row(llas.rows() / 2));
            polylines.emplace(index, std::move(llas));
        },
        [&](const mapbox::geojson::point &g) {
            positions.push_back({g.x, g.y, g.z});
        },
        [&](const mapbox::geojson::multi_point &g) {
            Eigen::Vector3d p =
                Eigen::Map<const RowVectors>(&g[0].x, g.size(), 3)
                    .colwise()
                    .mean();
            positions.push_back(p);
        },
        [&](const mapbox::geojson::polygon &g) {
            auto &ls = g[0];
            RowVectors src =
                Eigen::Map<const RowVectors>(&ls[0].x, ls.size(), 3);
            auto llas = douglas_simplify(src, 1.0, true);
            positions.push_back(llas.row(llas.rows() / 2));
            polylines.emplace(index, std::move(llas));
        },
        [&](const mapbox::geojson::multi_line_string &g) {
            auto &ls = g[0];
            RowVectors src =
                Eigen::Map<const RowVectors>(&ls[0].x, ls.size(), 3);
            auto llas = douglas_simplify(src, 1.0, true);
            positions.push_back(llas.row(llas.rows() / 2));
            polylines.emplace(index, std::move(llas));
        },
        [&](const mapbox::geojson::multi_polygon &g) {
            auto &ls = g[0][0];
            RowVectors src =
                Eigen::Map<const RowVectors>(&ls[0].x, ls.size(), 3);
            auto llas = douglas_simplify(src, 1.0, true);
            positions.push_back(llas.row(llas.rows() / 2));
            polylines.emplace(index, std::move(llas));
        },
        [&](const mapbox::geojson::geometry_collection &g) {
            index_geometry(index, g[0], positions, polylines);
        },
        [&](const auto &g) {
            throw std::invalid_argument(
                fmt::format("failed to index {}th geometry", index));
        });
}

inline RapidjsonValue
row_vectors_to_json(const Eigen::Ref<const RowVectors> &coords,
                    RapidjsonAllocator &allocator)
{
    RapidjsonValue arr(rapidjson::kArrayType);
    arr.Reserve(coords.rows(), allocator);
    for (int i = 0, N = coords.rows(); i < N; ++i) {
        RapidjsonValue xyz(rapidjson::kArrayType);
        xyz.Reserve(3, allocator);
        xyz.PushBack(RapidjsonValue(coords(i, 0)), allocator);
        xyz.PushBack(RapidjsonValue(coords(i, 1)), allocator);
        xyz.PushBack(RapidjsonValue(coords(i, 2)), allocator);
        arr.PushBack(xyz, allocator);
    }
    return arr;
}

RapidjsonValue
index_geojson(const mapbox::geojson::feature_collection &_features)
{
    RapidjsonAllocator allocator;

    mapbox::geojson::value::array_type ids;
    ids.reserve(_features.size());
    std::vector<Eigen::Vector3d> positions;
    positions.reserve(_features.size());
    std::map<int, RowVectors> polylines;
    for (int i = 0; i < _features.size(); ++i) {
        auto &f = _features[i];
        index_geometry(i, f.geometry, positions, polylines);
        auto &props = f.properties;
        auto id_itr = props.find("id");
        if (id_itr != props.end() && id_itr->second.is<std::string>()) {
            ids.push_back(id_itr->second);
        } else if (f.id.is<std::string>()) {
            ids.push_back(f.id.get<std::string>());
        } else {
            ids.push_back(mapbox::feature::null_value);
        }
    }
    RapidjsonValue index(rapidjson::kObjectType);
    index.AddMember(
        "features.properties.ids",
        mapbox::geojson::value::visit(mapbox::geojson::value(std::move(ids)),
                                      mapbox::geojson::to_value{allocator}),
        allocator);
    index.AddMember(
        "features.geometry.positions",
        row_vectors_to_json(
            Eigen::Map<const RowVectors>(&positions[0][0], positions.size(), 3),
            allocator),
        allocator);
    {
        RapidjsonValue j(rapidjson::kObjectType);
        j.MemberReserve(polylines.size(), allocator);
        for (auto &pair : polylines) {
            auto idx = std::to_string(pair.first);
            j.AddMember(RapidjsonValue(idx.c_str(), idx.size(), allocator),
                        row_vectors_to_json(pair.second, allocator), allocator);
        }
        index.AddMember("features.geometry.polylines", j, allocator);
    }
    for (auto &pair : _features.custom_properties) {
        auto &key = pair.first;
        if (key.empty() || key.back() != 's') {
            continue;
        }
        auto &items = pair.second;
        if (!items.is<mapbox::geojson::value::array_type>()) {
            continue;
        }
        auto &arr = items.get<mapbox::geojson::value::array_type>();
        RapidjsonValue ids(rapidjson::kArrayType);
        ids.Reserve(arr.size(), allocator);
        if (key == "observations") {
            for (auto &e : arr) {
                auto &o = e.get<mapbox::geojson::value::object_type>();
                RapidjsonValue _ids(rapidjson::kArrayType);
                for (auto &k :
                     std::vector<std::string>{"frame_id", "landmark_id"}) {
                    auto itr = o.find(k);
                    if (itr == o.end() || !itr->second.is<std::string>()) {
                        continue;
                    }
                    auto &v = itr->second.get<std::string>();
                    _ids.PushBack(
                        RapidjsonValue(v.c_str(), v.size(), allocator),
                        allocator);
                }
                ids.PushBack(_ids, allocator);
            }
        } else {
            for (auto &e : arr) {
                if (!e.is<mapbox::geojson::value::object_type>()) {
                    ids.PushBack(RapidjsonValue(), allocator);
                    continue;
                }
                auto &o = e.get<mapbox::geojson::value::object_type>();
                auto itr = o.find("id");
                if (itr == o.end() || !itr->second.is<std::string>()) {
                    ids.PushBack(RapidjsonValue(), allocator);
                    continue;
                }
                auto &id = itr->second.get<std::string>();
                ids.PushBack(RapidjsonValue(id.c_str(), id.size(), allocator),
                             allocator);
            }
        }
        index.AddMember(RapidjsonValue(key.c_str(), key.size(), allocator), ids,
                        allocator);
    }

    return index;
}

inline uint64_t h3index(int resolution, double lon, double lat)
{
    LatLng coord;
    coord.lng = lon / 180.0 * M_PI;
    coord.lat = lat / 180.0 * M_PI;
    H3Index idx;
    latLngToCell(&coord, resolution, &idx);
    return idx;
}

inline std::set<uint64_t>
h3index(int resolution, const std::vector<mapbox::geojson::point> &geometry)
{
    std::set<uint64_t> ret;
    for (auto &g : geometry) {
        ret.insert(h3index(resolution, g.x, g.y));
    }
    return ret;
}

inline std::set<uint64_t> h3index(int resolution,
                                  const mapbox::geojson::geometry &geometry)
{
    std::set<uint64_t> ret;
    geometry.match(
        [&](const mapbox::geojson::line_string &ls) {
            auto cur = h3index(resolution, ls);
            ret.insert(cur.begin(), cur.end());
        },
        [&](const mapbox::geojson::multi_point &mp) {
            auto cur = h3index(resolution, mp);
            ret.insert(cur.begin(), cur.end());
        },
        [&](const mapbox::geojson::point &p) {
            ret.insert(h3index(resolution, p.x, p.y));
        },
        [&](const mapbox::geojson::multi_line_string &mls) {
            for (auto &ls : mls) {
                auto cur = h3index(resolution, ls);
                ret.insert(cur.begin(), cur.end());
            }
        },
        [&](const mapbox::geojson::polygon &g) {
            for (auto &r : g) {
                auto cur = h3index(resolution, r);
                ret.insert(cur.begin(), cur.end());
            }
        },
        [&](const mapbox::geojson::multi_polygon &g) {
            for (auto &p : g) {
                for (auto &r : p) {
                    auto cur = h3index(resolution, r);
                    ret.insert(cur.begin(), cur.end());
                }
            }
        },
        [&](const mapbox::geojson::geometry_collection &gc) {
            for (auto &g : gc) {
                auto cur = h3index(resolution, g);
                ret.insert(cur.begin(), cur.end());
            }
        },
        [&](const auto &g) {
            //
        });
    return ret;
}

RapidjsonValue
strip_geojson(const mapbox::geojson::feature_collection &_features,
              const CondenseOptions &options)
{
    auto h3_type_idx =
        std::unordered_map<uint64_t,
                           std::unordered_map<std::string, std::vector<int>>>{};
    auto selected = std::set<int>{};
    for (int i = 0; i < _features.size(); ++i) {
        auto &f = _features[i];
        auto itr = f.properties.find("type");
        if (itr == f.properties.end() || !itr->second.is<std::string>()) {
            selected.insert(i); // keep all features without type
            continue;
        }
        auto &type = itr->second.get<std::string>();
        auto h3idxes = h3index(options.sparsify_h3_resolution, f.geometry);
        for (auto h3idx : h3idxes) {
            h3_type_idx[h3idx][type].push_back(i);
        }
    }
    auto rng = std::default_random_engine{0}; // seed 0
    for (auto &pair : h3_type_idx) {
        for (auto &id_idx : pair.second) {
            auto &idxes = id_idx.second;
            if (idxes.size() <= options.sparsify_upper_limit) {
                selected.insert(idxes.begin(), idxes.end());
            } else {
                std::shuffle(idxes.begin(), idxes.end(), rng);
                selected.insert(idxes.begin(),
                                idxes.begin() + options.sparsify_upper_limit);
            }
        }
    }

    RapidjsonAllocator allocator;
    RapidjsonValue features(rapidjson::kArrayType);
    features.Reserve(selected.size(), allocator);
    for (int index = 0, N = _features.size(); index < N; ++index) {
        if (selected.find(index) == selected.end()) {
            continue;
        }
        auto &f = _features[index];
        RapidjsonValue feature(rapidjson::kObjectType);
        feature.AddMember("type", "Feature", allocator);
        RapidjsonValue properties(rapidjson::kObjectType);
        f.geometry.match(
            [&](const mapbox::geojson::line_string &ls) {
                RowVectors src =
                    Eigen::Map<const RowVectors>(&ls[0].x, ls.size(), 3);
                auto llas =
                    douglas_simplify(src, options.douglas_epsilon, true);
                mapbox::geojson::line_string geom;
                geom.resize(llas.rows());
                Eigen::Map<RowVectors>(&geom[0].x, geom.size(), 3) = llas;
                feature.AddMember(
                    "geometry",
                    mapbox::geojson::convert(
                        mapbox::geojson::geometry{std::move(geom)}, allocator),
                    allocator);
            },
            [&](const mapbox::geojson::multi_point &mp) {
                Eigen::Map<const RowVectors> llas(&mp[0].x, mp.size(), 3);
                const auto enus = lla2enu(llas);
                Eigen::Vector3d center = llas.colwise().mean();
                auto geom = mapbox::geojson::convert(
                    mapbox::geojson::geometry{mapbox::geojson::point{
                        center[0], center[1], center[2]}},
                    allocator);
                feature.AddMember("geometry", geom, allocator);
                Eigen::Array3d span = Eigen::round((enus.colwise().maxCoeff() -
                                                    enus.colwise().minCoeff())
                                                       .array() *
                                                   100.0) /
                                      100.0;
                if (!span.isZero()) {
                    RapidjsonValue span_xyz(rapidjson::kArrayType);
                    span_xyz.Reserve(3, allocator);
                    span_xyz.PushBack(RapidjsonValue(span[0]), allocator);
                    span_xyz.PushBack(RapidjsonValue(span[1]), allocator);
                    span_xyz.PushBack(RapidjsonValue(span[2]), allocator);
                    properties.AddMember("size", span_xyz, allocator);
                }
            },
            [&](const auto &g) {
                auto geom = mapbox::geojson::convert(f.geometry, allocator);
                feature.AddMember("geometry", geom, allocator);
            });
        auto type_itr = f.properties.find("type");
        if (type_itr != f.properties.end() &&
            type_itr->second.is<std::string>()) {
            auto &type = type_itr->second.get<std::string>();
            properties.AddMember(
                "type",                                               //
                RapidjsonValue(type.c_str(), type.size(), allocator), //
                allocator);
        }
        properties.AddMember("index", RapidjsonValue(index), allocator);
        setup_extrinsic_to_heading_pitch_roll(f.properties, properties,
                                              allocator);
        feature.AddMember("properties", properties, allocator);
        features.PushBack(feature, allocator);
    }
    RapidjsonValue geojson(rapidjson::kObjectType);
    geojson.AddMember("type", "FeatureCollection", allocator);
    geojson.AddMember("features", features, allocator);
    if (options.debug) {
        std::map<std::string, int> type2count;
        for (auto &pair : h3_type_idx) {
            for (auto &t2i : pair.second) {
                auto &t = t2i.first;
                type2count[t] = std::max(type2count[t], (int)t2i.second.size());
            }
        }
        auto stats = mapbox::geojson::value::object_type{};
        for (auto &pair : type2count) {
            stats[pair.first] = pair.second;
        }
        geojson.AddMember("h3_cell_statistics",
                          mapbox::geojson::value::visit(
                              mapbox::geojson::value{std::move(stats)},
                              mapbox::geojson::to_value{allocator}),
                          allocator);
    }
    return geojson;
}

bool gridify_geojson(const mapbox::geojson::feature_collection &features,
                     const std::string &output_grids_dir,
                     const CondenseOptions &options)
{
    std::unordered_map<int, std::set<uint64_t>> index2h3index;
    std::unordered_map<uint64_t, std::vector<int>> h3index2index;
    for (int i = 0; i < features.size(); ++i) {
        auto &f = features[i];
        auto h3idxes = h3index(options.grid_h3_resolution, f.geometry);
        for (auto h3idx : h3idxes) {
            h3index2index[h3idx].push_back(i);
        }
        index2h3index.emplace(i, std::move(h3idxes));
    }
    mapbox::geojson::feature_collection copy;
    const mapbox::geojson::feature_collection *fc_ptr = &copy;
    if (options.grid_features_keep_properties) {
        fc_ptr = &features;
    } else {
        copy.reserve(features.size());
        for (auto &f : features) {
            auto ff = mapbox::geojson::feature{f.geometry};
            auto type_itr = f.properties.find("type");
            if (type_itr != f.properties.end()) {
                ff.properties.emplace("type", type_itr->second);
            }
            auto id_itr = f.properties.find("id");
            if (id_itr != f.properties.end()) {
                ff.properties.emplace("id", id_itr->second);
            }
            auto stroke_itr = f.properties.find("stroke");
            if (stroke_itr != f.properties.end()) {
                ff.properties.emplace("stroke", stroke_itr->second);
            }
            copy.emplace_back(std::move(ff));
        }
    }
    for (const auto &pair : h3index2index) {
        auto h3idx = pair.first;
        auto &indexes = pair.second;
        auto fc = mapbox::geojson::feature_collection{};
        fc.reserve(indexes.size());
        for (auto idx : indexes) {
            fc.push_back((*fc_ptr)[idx]);
        }

        RapidjsonAllocator allocator;
        auto json = mapbox::geojson::convert(fc, allocator);
        int i = -1;
        for (auto idx : indexes) {
            auto &props = json["features"][++i]["properties"];
            setup_extrinsic_to_heading_pitch_roll(features[idx].properties, //
                                                  props, allocator);
            props.GetObject().AddMember("index", RapidjsonValue(idx),
                                        allocator);
            if (options.debug) {
                auto &h3idxes = index2h3index.at(idx);
                RapidjsonValue arr(rapidjson::kArrayType);
                arr.Reserve(h3idxes.size(), allocator);
                for (auto h : h3idxes) {
                    auto hex = fmt::format("{:016x}", h);
                    arr.PushBack(
                        RapidjsonValue(hex.c_str(), hex.size(), allocator),
                        allocator);
                }
                props.GetObject().AddMember("h3index", arr, allocator);
            }
        }
        if (options.sort_keys) {
            sort_keys_inplace(json);
        }
        std::string path =
            fmt::format("{}/h3_cell_{}_{:016x}.json", output_grids_dir,
                        options.grid_h3_resolution, h3idx);
        spdlog::info("writing {} features to {}", fc.size(), path);
        if (!dump_json(path, json, options.indent)) {
            spdlog::error("failed to write {} features (h3idx: {}) to {}",
                          fc.size(), h3idx, path);
            return false;
        }
    }
    return true;
}

bool condense_geojson(const std::string &input_path,
                      const std::optional<std::string> &output_index_path,
                      const std::optional<std::string> &output_strip_path,
                      const std::optional<std::string> &output_grids_dir,
                      const CondenseOptions &options)
{
    if (!output_index_path && !output_strip_path && !output_grids_dir) {
        spdlog::error("should specify either --output_index_path, "
                      "--output_strip_path or --output_grids_dir");
        return false;
    }
    auto json = load_json(input_path);
    if (!json.IsObject()) {
        spdlog::error("failed to load {}", input_path);
        return false;
    }
    auto geojson = mapbox::geojson::convert(json);
    if (geojson.is<mapbox::geojson::geometry>()) {
        geojson = mapbox::geojson::feature_collection{
            mapbox::geojson::feature{geojson.get<mapbox::geojson::geometry>()}};
    } else if (geojson.is<mapbox::geojson::feature>()) {
        geojson = mapbox::geojson::feature_collection{
            {geojson.get<mapbox::geojson::feature>()}};
    }
    auto &features = geojson.get<mapbox::geojson::feature_collection>();
    if (features.empty()) {
        spdlog::error("not any features in {}", input_path);
        return false;
    }
    if (output_index_path) {
        auto index = index_geojson(features);
        if (options.sort_keys) {
            sort_keys_inplace(index);
        }
        spdlog::info("writing to {}", *output_index_path);
        if (!dump_json(*output_index_path, index, options.indent)) {
            spdlog::error("failed to dump to {}", *output_index_path);
            return false;
        }
    }
    if (output_strip_path) {
        auto stripped = strip_geojson(features, options);
        if (options.sort_keys) {
            sort_keys_inplace(stripped);
        }
        spdlog::info("writing to {}", *output_strip_path);
        if (!dump_json(*output_strip_path, stripped, options.indent)) {
            spdlog::error("failed to dump to {}", *output_strip_path);
            return false;
        }
    }
    if (output_grids_dir) {
        return gridify_geojson(features, *output_grids_dir, options);
    }
    return true;
}

bool dissect_geojson(const std::string &input_path,
                     const std::optional<std::string> &output_geometry,
                     const std::optional<std::string> &output_properties,
                     const std::optional<std::string> &output_observations,
                     const std::optional<std::string> &output_others,
                     bool indent = false)
{
    if (!output_geometry && !output_properties && !output_observations &&
        !output_others) {
        spdlog::error(
            "should specify either --output_geometry, "
            "--output_properties, --output_observations or --output_others");
        return false;
    }
    auto json = load_json(input_path);
    if (!json.IsObject()           //
        || !json.HasMember("type") //
        || "FeatureCollection" != std::string{json["type"].GetString(),
                                              json["type"].GetStringLength()}) {
        spdlog::error("not valid geojson FeatureCollection data: {}",
                      input_path);
        return false;
    }
    RapidjsonAllocator allocator;
    auto &features = json["features"];
    if (output_geometry) {
        RapidjsonValue geometry(rapidjson::kArrayType);
        for (auto &f : features.GetArray()) {
            geometry.PushBack(f["geometry"], allocator);
        }
        spdlog::info("writing to {}", *output_geometry);
        if (!dump_json(*output_geometry, geometry, indent)) {
            spdlog::error("failed to dump to {}", *output_geometry);
            return false;
        }
    }
    if (output_properties) {
        RapidjsonValue properties(rapidjson::kArrayType);
        properties.Reserve(features.Size(), allocator);
        for (auto &f : features.GetArray()) {
            properties.PushBack(f["properties"], allocator);
        }
        spdlog::info("writing to {}", *output_properties);
        if (!dump_json(*output_properties, properties, indent)) {
            spdlog::error("failed to dump to {}", *output_properties);
            return false;
        }
    }
    if (output_observations && json.HasMember("observations")) {
        spdlog::info("writing to {}", *output_observations);
        if (!dump_json(*output_observations, json["observations"], indent)) {
            spdlog::error("failed to dump to {}", *output_observations);
            return false;
        }
    }
    if (!output_others) {
        return true;
    }
    json.EraseMember("type");
    json.EraseMember("features");
    json.EraseMember("observations");
    spdlog::info("writing to {}", *output_others);
    if (!dump_json(*output_others, json, indent)) {
        spdlog::error("failed to dump to {}", *output_others);
        return false;
    }
    return true;
}
} // namespace cubao

PYBIND11_MODULE(_core, m)
{
    using namespace cubao;
    py::class_<CondenseOptions>(m, "CondenseOptions", py::module_local()) //
        .def(py::init<>(), "Default constructor for CondenseOptions")
        .def_readwrite("douglas_epsilon", &CondenseOptions::douglas_epsilon,
                       "Epsilon value for Douglas-Peucker algorithm")
        .def_readwrite("grid_h3_resolution",
                       &CondenseOptions::grid_h3_resolution,
                       "H3 resolution for grid features")
        .def_readwrite("indent", &CondenseOptions::indent,
                       "Indentation option for JSON output")
        .def_readwrite("sort_keys", &CondenseOptions::sort_keys,
                       "Option to sort keys in JSON output")
        .def_readwrite("grid_features_keep_properties",
                       &CondenseOptions::grid_features_keep_properties,
                       "Option to keep properties for grid features")
        .def_readwrite("sparsify_h3_resolution",
                       &CondenseOptions::sparsify_h3_resolution,
                       "H3 resolution for sparsification")
        .def_readwrite("sparsify_upper_limit",
                       &CondenseOptions::sparsify_upper_limit,
                       "Upper limit for sparsification")
        .def_readwrite("debug", &CondenseOptions::debug, "Debug option")
        //
        ;

    m.def("condense_geojson", &condense_geojson, //
          py::kw_only(),                         //
          "input_path"_a,                        //
          "output_index_path"_a = std::nullopt,  //
          "output_strip_path"_a = std::nullopt,  //
          "output_grids_dir"_a = std::nullopt,   //
          "options"_a = CondenseOptions(),       //
          R"docstring(
          Condense GeoJSON data.

          Args:
              input_path: Path to the input GeoJSON file.
              output_index_path: Optional path for the output index file.
              output_strip_path: Optional path for the output strip file.
              output_grids_dir: Optional directory for output grid files.
              options: CondenseOptions object with configuration options.

          Returns:
              bool: True if the operation was successful, False otherwise.
          )docstring")
        //
        ;

    m.def("dissect_geojson", &dissect_geojson,    //
          py::kw_only(),                          //
          "input_path"_a,                         //
          "output_geometry"_a = std::nullopt,     //
          "output_properties"_a = std::nullopt,   //
          "output_observations"_a = std::nullopt, //
          "output_others"_a = std::nullopt,       //
          "indent"_a = false,                     //
          R"docstring(
          Dissect GeoJSON data into separate components.

          Args:
              input_path: Path to the input GeoJSON file.
              output_geometry: Optional path for the output geometry file.
              output_properties: Optional path for the output properties file.
              output_observations: Optional path for the output observations file.
              output_others: Optional path for other output data.
              indent: Boolean flag to enable indentation in output JSON.

          Returns:
              bool: True if the operation was successful, False otherwise.
          )docstring")
        //
        ;

#ifdef VERSION_INFO
    m.attr("__version__") = MACRO_STRINGIFY(VERSION_INFO);
#else
    m.attr("__version__") = "dev";
#endif
}
