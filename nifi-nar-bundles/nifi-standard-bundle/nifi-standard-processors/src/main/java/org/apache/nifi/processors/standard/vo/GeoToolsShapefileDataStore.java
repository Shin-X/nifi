package org.apache.nifi.processors.standard.vo;

import org.geotools.data.shapefile.ShapefileDataStore;

import java.net.URL;

/*
 *描述:
 * @author liuxin
 * @date 2021/5/8 11:24
 * @param null:
 * @return
 */
public class GeoToolsShapefileDataStore implements AutoCloseable {

    public GeoToolsShapefileDataStore(URL param) {
        this.shapefileDataStore = new ShapefileDataStore(param);
    }

    private ShapefileDataStore shapefileDataStore;

    public ShapefileDataStore getShapefileDataStore() {
        return this.shapefileDataStore;
    }

    @Override
    public void close() {
        if (null != this.shapefileDataStore) {
            this.shapefileDataStore.dispose();
        }
    }
}
