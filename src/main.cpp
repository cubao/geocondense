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

#include <unordered_map>
#include <set>

#define STRINGIFY(x) #x
#define MACRO_STRINGIFY(x) STRINGIFY(x)

namespace py = pybind11;
using namespace pybind11::literals;
using namespace cubao;

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
    int h3_resolution = 8; // https://h3geo.org/docs/core-library/restable/
    bool indent = false;
    bool sort_keys = false;
    bool grid_features_keep_properties = false;
};

inline void index_geometry(int index, const mapbox::geojson::geometry &geom,
                           std::vector<Eigen::Vector3d> &positions,
                           std::map<int, RowVectors> &polylines)
{
    geom.match(
        [&](const mapbox::geojson::line_string &ls) {
            auto llas = douglas_simplify(
                Eigen::Map<const RowVectors>(&ls[0].x, ls.size(), 3), 1.0,
                true);
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
            auto llas = douglas_simplify(
                Eigen::Map<const RowVectors>(&ls[0].x, ls.size(), 3), 1.0,
                true);
            positions.push_back(llas.row(llas.rows() / 2));
            polylines.emplace(index, std::move(llas));
        },
        [&](const mapbox::geojson::multi_line_string &g) {
            auto &ls = g[0];
            auto llas = douglas_simplify(
                Eigen::Map<const RowVectors>(&ls[0].x, ls.size(), 3), 1.0,
                true);
            positions.push_back(llas.row(llas.rows() / 2));
            polylines.emplace(index, std::move(llas));
        },
        [&](const mapbox::geojson::multi_polygon &g) {
            auto &ls = g[0][0];
            auto llas = douglas_simplify(
                Eigen::Map<const RowVectors>(&ls[0].x, ls.size(), 3), 1.0,
                true);
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

    RapidjsonValue index(rapidjson::kObjectType);
    std::vector<Eigen::Vector3d> positions;
    std::map<int, RowVectors> polylines;
    for (int i = 0; i < _features.size(); ++i) {
        auto &f = _features[i];
        index_geometry(i, f.geometry, positions, polylines);
    }
    for (auto &pair : _features.custom_properties) {
    }
    return index;
}

RapidjsonValue
strip_geojson(const mapbox::geojson::feature_collection &_features,
              double douglas_epsilon)
{
    RapidjsonAllocator allocator;
    RapidjsonValue features(rapidjson::kArrayType);
    features.Reserve(_features.size(), allocator);
    int index = -1;
    for (auto &f : _features) {
        RapidjsonValue feature(rapidjson::kObjectType);
        feature.AddMember("type", "Feature", allocator);
        RapidjsonValue properties(rapidjson::kObjectType);
        f.geometry.match(
            [&](const mapbox::geojson::line_string &ls) {
                auto llas = douglas_simplify(
                    Eigen::Map<const RowVectors>(&ls[0].x, ls.size(), 3),
                    douglas_epsilon, true);
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
        properties.AddMember("index", RapidjsonValue(++index), allocator);
        setup_extrinsic_to_heading_pitch_roll(f.properties, properties,
                                              allocator);
        feature.AddMember("properties", properties, allocator);
        features.PushBack(feature, allocator);
    }
    RapidjsonValue geojson(rapidjson::kObjectType);
    geojson.AddMember("type", "FeatureCollection", allocator);
    geojson.AddMember("features", features, allocator);
    return geojson;
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

bool gridify_geojson(const mapbox::geojson::feature_collection &features,
                     const std::string &output_grids_dir,
                     const CondenseOptions &options)
{
    std::unordered_map<int, std::set<uint64_t>> index2h3index;
    std::unordered_map<uint64_t, std::vector<int>> h3index2index;
    for (int i = 0; i < features.size(); ++i) {
        auto &f = features[i];
        auto h3idxes = h3index(options.h3_resolution, f.geometry);
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
        }
        if (options.sort_keys) {
            sort_keys_inplace(json);
        }
        std::string path =
            fmt::format("{}/h3_cell_{}_{:016x}.json", output_grids_dir,
                        options.h3_resolution, h3idx);
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
        auto stripped = index_geojson(features);
    }
    if (output_strip_path) {
        auto stripped = strip_geojson(features, options.douglas_epsilon);
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

PYBIND11_MODULE(pybind11_geocondense, m)
{
    py::class_<CondenseOptions>(m, "CondenseOptions", py::module_local()) //
        .def(py::init<>())
        .def_readwrite("douglas_epsilon", &CondenseOptions::douglas_epsilon)
        .def_readwrite("h3_resolution", &CondenseOptions::h3_resolution)
        .def_readwrite("indent", &CondenseOptions::indent)
        .def_readwrite("sort_keys", &CondenseOptions::sort_keys)
        .def_readwrite("grid_features_keep_properties",
                       &CondenseOptions::grid_features_keep_properties)
        //
        ;

    m.def("condense_geojson", &condense_geojson, //
          py::kw_only(),                         //
          "input_path"_a,                        //
          "output_strip_path"_a = std::nullopt,  //
          "output_grids_dir"_a = std::nullopt,   //
          "options"_a = CondenseOptions())
        //
        ;

#ifdef VERSION_INFO
    m.attr("__version__") = MACRO_STRINGIFY(VERSION_INFO);
#else
    m.attr("__version__") = "dev";
#endif
}
