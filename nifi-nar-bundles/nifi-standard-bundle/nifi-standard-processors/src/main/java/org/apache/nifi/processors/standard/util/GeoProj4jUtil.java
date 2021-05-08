package org.apache.nifi.processors.standard.util;

import org.locationtech.proj4j.CRSFactory;
import org.locationtech.proj4j.CoordinateReferenceSystem;
import org.locationtech.proj4j.CoordinateTransformFactory;
import org.locationtech.proj4j.ProjCoordinate;

/**
 * proj4j
 *
 * @author zhangming
 */
public class GeoProj4jUtil {

    private GeoProj4jUtil() {
    }

    private static final String EPSG = "EPSG:";
    private static final CRSFactory crsFactory = new CRSFactory();
    private static final CoordinateTransformFactory coordinateTransformFactory = new CoordinateTransformFactory();

    public static ProjCoordinate transform(CoordinateReferenceSystem crsSource, CoordinateReferenceSystem crsTarget, ProjCoordinate source) {
        return coordinateTransformFactory.createTransform(crsSource, crsTarget).transform(source, new ProjCoordinate());
    }

    public static CoordinateReferenceSystem getCRS(int epsg) {
        return crsFactory.createFromName(EPSG + epsg);
    }

    public static ProjCoordinate getCoordinate(double x, double y) {
        return new ProjCoordinate(x, y);
    }
}
