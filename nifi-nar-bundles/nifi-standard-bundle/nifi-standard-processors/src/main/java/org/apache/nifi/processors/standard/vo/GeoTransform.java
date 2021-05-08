package org.apache.nifi.processors.standard.vo;



import org.apache.nifi.processors.standard.util.GeoProj4jUtil;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.*;
import org.locationtech.proj4j.CoordinateReferenceSystem;
import org.locationtech.proj4j.ProjCoordinate;

/*
 *描述:
 * @author liuxin
 * @date 2021/5/8 11:24
 * @param null:
 * @return
 */
public class GeoTransform {

    private static GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
    private CoordinateReferenceSystem proj4jCrsSource;
    private CoordinateReferenceSystem proj4jCrsTarget;

    public GeoTransform(int sourceSrid, int targetSrid) {
        this.proj4jCrsSource = GeoProj4jUtil.getCRS(sourceSrid);
        this.proj4jCrsTarget = GeoProj4jUtil.getCRS(targetSrid);
    }

    public GeoTransform(CoordinateReferenceSystem proj4jCrsSource, CoordinateReferenceSystem proj4jCrsTarget) {
        this.proj4jCrsSource = proj4jCrsSource;
        this.proj4jCrsTarget = proj4jCrsTarget;
    }

    /**
     * 转换
     *
     * @param geometry 几何对象
     * @return Geometry
     */
    public Geometry transform(Geometry geometry) {
        if (geometry instanceof Point) {
            return tranformPoint((Point) geometry);
        } else if (geometry instanceof MultiPoint) {
            return tranformMultiPoint((MultiPoint) geometry);
        } else if (geometry instanceof LineString) {
            return tranformLineString((LineString) geometry);
        } else if (geometry instanceof MultiLineString) {
            return tranformMultiLineString((MultiLineString) geometry);
        } else if (geometry instanceof Polygon) {
            return tranformPolygon((Polygon) geometry);
        } else if (geometry instanceof MultiPolygon) {
            return tranformMultiPolygon((MultiPolygon) geometry);
        } else if (geometry instanceof GeometryCollection) {
            return tranformGeometryCollection((GeometryCollection) geometry);
        } else {
            return null;
        }
    }

    /**
     * 点
     *
     * @param point 点
     * @return Point
     */
    private Point tranformPoint(Point point) {
        return geometryFactory.createPoint(transformCoordinate(point.getCoordinate()));
    }

    /**
     * 线
     *
     * @param lineString 线
     * @return LineString
     */
    private LineString tranformLineString(LineString lineString) {
        return geometryFactory.createLineString(tranformCoordinates(lineString.getCoordinates()));
    }

    /**
     * 面
     *
     * @param polygon 面
     * @return Polygon
     */
    private Polygon tranformPolygon(Polygon polygon) {
        LinearRing exteriorRing = geometryFactory.createLinearRing(tranformLineString(polygon.getExteriorRing()).getCoordinates());
        LinearRing interiorRings[] = new LinearRing[polygon.getNumInteriorRing()];

        for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
            interiorRings[i] = geometryFactory.createLinearRing(tranformLineString(polygon.getInteriorRingN(i)).getCoordinates());
        }

        return geometryFactory.createPolygon(exteriorRing, interiorRings);
    }

    /**
     * 多点
     *
     * @param multiPoint 多点
     * @return MultiPoint
     */
    private MultiPoint tranformMultiPoint(MultiPoint multiPoint) {
        return geometryFactory.createMultiPointFromCoords(tranformCoordinates(multiPoint.getCoordinates()));
    }

    /**
     * 多线
     *
     * @param multiLineString 多线
     * @return MultiLineString
     */
    private MultiLineString tranformMultiLineString(MultiLineString multiLineString) {
        LineString lineStrings[] = new LineString[multiLineString.getNumGeometries()];

        for (int i = 0; i < multiLineString.getNumGeometries(); i++) {
            lineStrings[i] = tranformLineString((LineString) multiLineString.getGeometryN(i));
        }

        return geometryFactory.createMultiLineString(lineStrings);
    }

    /**
     * 多面
     *
     * @param multiPolygon 多面
     * @return MultiPolygon
     */
    private MultiPolygon tranformMultiPolygon(MultiPolygon multiPolygon) {
        Polygon polygons[] = new Polygon[multiPolygon.getNumGeometries()];

        for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
            polygons[i] = tranformPolygon((Polygon) multiPolygon.getGeometryN(i));
        }

        return geometryFactory.createMultiPolygon(polygons);
    }

    /**
     * 几何集
     *
     * @param geometryCollection 几何集
     * @return GeometryCollection
     */
    private GeometryCollection tranformGeometryCollection(GeometryCollection geometryCollection) {
        Geometry geometries[] = new Geometry[geometryCollection.getNumGeometries()];

        for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
            Geometry geometry = geometryCollection.getGeometryN(i);

            if (geometry instanceof Point) {
                geometries[i] = tranformPoint((Point) geometry);
            } else if (geometry instanceof MultiPoint) {
                geometries[i] = tranformMultiPoint((MultiPoint) geometry);
            } else if (geometry instanceof LineString) {
                geometries[i] = tranformLineString((LineString) geometry);
            } else if (geometry instanceof MultiLineString) {
                geometries[i] = tranformMultiLineString((MultiLineString) geometry);
            } else if (geometry instanceof Polygon) {
                geometries[i] = tranformPolygon((Polygon) geometry);
            } else if (geometry instanceof MultiPolygon) {
                geometries[i] = tranformMultiPolygon((MultiPolygon) geometry);
            }
        }

        return geometryFactory.createGeometryCollection(geometries);
    }

    /**
     * 坐标集
     *
     * @param coordinates 坐标集
     * @return Coordinate[]
     */
    private Coordinate[] tranformCoordinates(Coordinate coordinates[]) {
        Coordinate outCoordinates[] = new Coordinate[coordinates.length];

        for (int i = 0; i < coordinates.length; i++) {
            outCoordinates[i] = transformCoordinate(coordinates[i]);
        }

        return outCoordinates;
    }

    /**
     * 坐标
     *
     * @param coordinate 坐标
     * @return Coordinate
     */
    public Coordinate transformCoordinate(Coordinate coordinate) {
        ProjCoordinate result = GeoProj4jUtil.transform(this.proj4jCrsSource, this.proj4jCrsTarget, GeoProj4jUtil.getCoordinate(coordinate.x, coordinate.y));

        return new Coordinate(result.x, result.y);
    }
}
