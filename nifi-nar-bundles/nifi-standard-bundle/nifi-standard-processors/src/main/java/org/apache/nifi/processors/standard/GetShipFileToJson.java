/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.vo.GeoToolsShapefileDataStore;
import org.apache.nifi.processors.standard.vo.GeoTransform;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBWriter;
import org.opengis.feature.Feature;
import org.opengis.feature.Property;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"local", "files", "filesystem", "ingest", "ingress", "get", "source", "input","geotools","geo"})
@CapabilityDescription("???????????????????????????????????????NiFi?????????????????????????????????????????????")
@WritesAttributes({
    @WritesAttribute(attribute = "filename", description = "The filename is set to the name of the file on disk"),
    @WritesAttribute(attribute = "path", description = "The path is set to the relative path of the file's directory on disk. For example, "
            + "if the <Input Directory> property is set to /tmp, files picked up from /tmp will have the path attribute set to ./. If "
            + "the <Recurse Subdirectories> property is set to true and a file is picked up from /tmp/abc/1/2/3, then the path attribute will "
            + "be set to abc/1/2/3"),
    @WritesAttribute(attribute = "file.creationTime", description = "The date and time that the file was created. May not work on all file systems"),
    @WritesAttribute(attribute = "file.lastModifiedTime", description = "The date and time that the file was last modified. May not work on all "
            + "file systems"),
    @WritesAttribute(attribute = "file.lastAccessTime", description = "The date and time that the file was last accessed. May not work on all "
            + "file systems"),
    @WritesAttribute(attribute = "file.owner", description = "The owner of the file. May not work on all file systems"),
    @WritesAttribute(attribute = "file.group", description = "The group owner of the file. May not work on all file systems"),
    @WritesAttribute(attribute = "file.permissions", description = "The read/write/execute permissions of the file. May not work on all file systems"),
    @WritesAttribute(attribute = "absolute.path", description = "The full/absolute path from where a file was picked up. The current 'path' "
            + "attribute is still populated, but may be a relative path")})
@SeeAlso({PutFile.class, FetchFile.class})
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.READ_FILESYSTEM,
                        explanation = "Provides operator the ability to read from any file that NiFi has access to."),
                @Restriction(
                        requiredPermission = RequiredPermission.WRITE_FILESYSTEM,
                        explanation = "Provides operator the ability to delete any file that NiFi has access to.")
        }
)
public class GetShipFileToJson extends AbstractProcessor {

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
//            .name("Input Directory")
            .name("????????????")
            .description("?????????????????????????????????")
            .required(true)
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor RECURSE = new PropertyDescriptor.Builder()
//            .name("Recurse Subdirectories")
            .name("???????????????")
            .description("???????????????????????????????????????")
//            .description("Indicates whether or not to pull files from subdirectories")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor KEEP_SOURCE_FILE = new PropertyDescriptor.Builder()
//            .name("Keep Source File")
            .name("???????????????")
            .description("?????????????????????????????????????????????????????????????????????;????????????????????????????????????????????????????????????????????????????????????????????????NiFi???????????????????????????????????????????????????????????????????????????????????????")
//            .description("If true, the file is not deleted after it has been copied to the Content Repository; "
//                    + "this causes the file to be picked up continually and is useful for testing purposes.  "
//                    + "If not keeping original NiFi will need write permissions on the directory it is pulling "
//                    + "from otherwise it will ignore the file.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor FILE_FILTER = new PropertyDescriptor.Builder()
//            .name("File Filter")
            .name("????????????")
            .description("??????????????????????????????????????????????????????????????????")
//            .description("Only files whose names match the given regular expression will be picked up")
            .required(true)
            .defaultValue("[^\\.].*")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();
    public static final PropertyDescriptor PATH_FILTER = new PropertyDescriptor.Builder()
//            .name("Path Filter")
            .name("????????????")
            .description("??? " + RECURSE.getName() + " ??????, ?????????????????????????????????????????????????????????")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();
    public static final PropertyDescriptor MIN_AGE = new PropertyDescriptor.Builder()
            .name("??????????????????")
            .description("????????????????????????????????????;?????????????????????????????????(????????????????????????)???????????????")
//            .name("Minimum File Age")
//            .description("The minimum age that a file must be in order to be pulled; any file younger than this amount of time (according to last modification date) will be ignored")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
            .build();
    public static final PropertyDescriptor MAX_AGE = new PropertyDescriptor.Builder()
            .name("??????????????????")
            .description("??????????????????????????????????????????;?????????????????????????????????(???????????????????????????)???????????????")
//            .name("Maximum File Age")
//            .description("The maximum age that a file must be in order to be pulled; any file older than this amount of time (according to last modification date) will be ignored")
            .required(false)
            .addValidator(StandardValidators.createTimePeriodValidator(100, TimeUnit.MILLISECONDS, Long.MAX_VALUE, TimeUnit.NANOSECONDS))
            .build();
    public static final PropertyDescriptor MIN_SIZE = new PropertyDescriptor.Builder()
            .name("??????????????????")
            .description("???????????????????????????????????????")
//            .name("Minimum File Size")
//            .description("The minimum size that a file must be in order to be pulled")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("0 B")
            .build();
    public static final PropertyDescriptor MAX_SIZE = new PropertyDescriptor.Builder()
            .name("??????????????????")
            .description("?????????????????????????????????")
//            .name("Maximum File Size")
//            .description("The maximum size that a file can be in order to be pulled")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    public static final PropertyDescriptor SRID = new PropertyDescriptor.Builder()
            .name("SRID")
            .description("?????????????????????, ?????????")
//            .name("SRID")
//            .description("?????????????????????, ?????????")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .build();
    public static final PropertyDescriptor IGNORE_HIDDEN_FILES = new PropertyDescriptor.Builder()
            .name("??????????????????")
            .description("???????????????????????????????????????")
//            .name("Ignore Hidden Files")
//            .description("Indicates whether or not hidden files should be ignored")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();
    public static final PropertyDescriptor POLLING_INTERVAL = new PropertyDescriptor.Builder()
            .name("????????????")
            .description("??????????????????????????????????????????????????????")
//            .name("Polling Interval")
//            .description("Indicates how long to wait before performing a directory listing")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
            .build();
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
//            .name("Batch Size")
            .name("????????????")
            .description("???????????????????????????????????????")
//            .description("The maximum number of files to pull in each iteration")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();

    public static final String FILE_CREATION_TIME_ATTRIBUTE = "file.creationTime";
    public static final String FILE_LAST_MODIFY_TIME_ATTRIBUTE = "file.lastModifiedTime";
    public static final String FILE_LAST_ACCESS_TIME_ATTRIBUTE = "file.lastAccessTime";
    public static final String FILE_OWNER_ATTRIBUTE = "file.owner";
    public static final String FILE_GROUP_ATTRIBUTE = "file.group";
    public static final String FILE_PERMISSIONS_ATTRIBUTE = "file.permissions";
    public static final String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("??????").description("?????????????????????????????????").build();
//    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("All files are routed to success").build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private final AtomicReference<FileFilter> fileFilterRef = new AtomicReference<>();

    private final BlockingQueue<File> fileQueue = new LinkedBlockingQueue<>();
    private final Set<File> inProcess = new HashSet<>();    // guarded by queueLock
    private final Set<File> recentlyProcessed = new HashSet<>();    // guarded by queueLock
    private final Lock queueLock = new ReentrantLock();

    private final Lock listingLock = new ReentrantLock();

    private final AtomicLong queueLastUpdated = new AtomicLong(0L);

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DIRECTORY);
        properties.add(FILE_FILTER);
        properties.add(PATH_FILTER);
        properties.add(BATCH_SIZE);
        properties.add(KEEP_SOURCE_FILE);
        properties.add(RECURSE);
        properties.add(POLLING_INTERVAL);
        properties.add(IGNORE_HIDDEN_FILES);
        properties.add(MIN_AGE);
        properties.add(MAX_AGE);
        properties.add(MIN_SIZE);
        properties.add(MAX_SIZE);
        properties.add(SRID);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        fileFilterRef.set(createFileFilter(context));
        fileQueue.clear();
    }

    private FileFilter createFileFilter(final ProcessContext context) {
        final long minSize = context.getProperty(MIN_SIZE).asDataSize(DataUnit.B).longValue();
        final Double maxSize = context.getProperty(MAX_SIZE).asDataSize(DataUnit.B);
        final long minAge = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final Long maxAge = context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final boolean ignoreHidden = context.getProperty(IGNORE_HIDDEN_FILES).asBoolean();
        final Pattern filePattern = Pattern.compile(context.getProperty(FILE_FILTER).getValue());
        final String indir = context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue();
        final boolean recurseDirs = context.getProperty(RECURSE).asBoolean();
        final String pathPatternStr = context.getProperty(PATH_FILTER).getValue();
        final Pattern pathPattern = (!recurseDirs || pathPatternStr == null) ? null : Pattern.compile(pathPatternStr);
        final boolean keepOriginal = context.getProperty(KEEP_SOURCE_FILE).asBoolean();

        return new FileFilter() {
            @Override
            public boolean accept(final File file) {
                if (minSize > file.length()) {
                    return false;
                }
                if (maxSize != null && maxSize < file.length()) {
                    return false;
                }
                final long fileAge = System.currentTimeMillis() - file.lastModified();
                if (minAge > fileAge) {
                    return false;
                }
                if (maxAge != null && maxAge < fileAge) {
                    return false;
                }
                if (ignoreHidden && file.isHidden()) {
                    return false;
                }
                if (pathPattern != null) {
                    Path reldir = Paths.get(indir).relativize(file.toPath()).getParent();
                    if (reldir != null && !reldir.toString().isEmpty()) {
                        if (!pathPattern.matcher(reldir.toString()).matches()) {
                            return false;
                        }
                    }
                }
                //Verify that we have at least read permissions on the file we're considering grabbing
                if (!Files.isReadable(file.toPath())) {
                    return false;
                }

                //Verify that if we're not keeping original that we have write permissions on the directory the file is in
                if (keepOriginal == false && !Files.isWritable(file.toPath().getParent())) {
                    return false;
                }
                return filePattern.matcher(file.getName()).matches();
            }
        };
    }
    //????????????????????????file set??????
    private Set<File> performListing(final File directory, final FileFilter filter, final boolean recurseSubdirectories) {
        Path p = directory.toPath();
        if (!Files.isWritable(p) || !Files.isReadable(p)) {
            throw new IllegalStateException("Directory '" + directory + "' does not have sufficient permissions (i.e., not writable and readable)");
        }
        final Set<File> queue = new HashSet<>();
        if (!directory.exists()) {
            return queue;
        }

        final File[] children = directory.listFiles();
        if (children == null) {
            return queue;
        }

        for (final File child : children) {
            if (child.isDirectory()) {
                if (recurseSubdirectories) {
                    queue.addAll(performListing(child, filter, recurseSubdirectories));
                }
            } else if (filter.accept(child)) {
                queue.add(child);
            }
        }

        return queue;
    }

    protected Map<String, String> getAttributesFromFile(final Path file) {
        Map<String, String> attributes = new HashMap<>();
        try {
            FileStore store = Files.getFileStore(file);
            if (store.supportsFileAttributeView("basic")) {
                try {
                    final DateFormat formatter = new SimpleDateFormat(FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);
                    BasicFileAttributeView view = Files.getFileAttributeView(file, BasicFileAttributeView.class);
                    BasicFileAttributes attrs = view.readAttributes();
                    attributes.put(FILE_LAST_MODIFY_TIME_ATTRIBUTE, formatter.format(new Date(attrs.lastModifiedTime().toMillis())));
                    attributes.put(FILE_CREATION_TIME_ATTRIBUTE, formatter.format(new Date(attrs.creationTime().toMillis())));
                    attributes.put(FILE_LAST_ACCESS_TIME_ATTRIBUTE, formatter.format(new Date(attrs.lastAccessTime().toMillis())));
                } catch (Exception ignore) {
                } // allow other attributes if these fail
            }
            if (store.supportsFileAttributeView("owner")) {
                try {
                    FileOwnerAttributeView view = Files.getFileAttributeView(file, FileOwnerAttributeView.class);
                    attributes.put(FILE_OWNER_ATTRIBUTE, view.getOwner().getName());
                } catch (Exception ignore) {
                } // allow other attributes if these fail
            }
            if (store.supportsFileAttributeView("posix")) {
                try {
                    PosixFileAttributeView view = Files.getFileAttributeView(file, PosixFileAttributeView.class);
                    attributes.put(FILE_PERMISSIONS_ATTRIBUTE, PosixFilePermissions.toString(view.readAttributes().permissions()));
                    attributes.put(FILE_GROUP_ATTRIBUTE, view.readAttributes().group().getName());
                } catch (Exception ignore) {
                } // allow other attributes if these fail
            }
        } catch (IOException ioe) {
            // well then this FlowFile gets none of these attributes
        }

        return attributes;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final File directory = new File(context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue());
        final boolean keepingSourceFile = context.getProperty(KEEP_SOURCE_FILE).asBoolean();
        final ComponentLog logger = getLogger();
        final int targetSrid = context.getProperty(SRID).asInteger();
        if (fileQueue.size() < 100) {
            final long pollingMillis = context.getProperty(POLLING_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
            if ((queueLastUpdated.get() < System.currentTimeMillis() - pollingMillis) && listingLock.tryLock()) {
                try {
                    final Set<File> listing = performListing(directory, fileFilterRef.get(), context.getProperty(RECURSE).asBoolean().booleanValue());

                    queueLock.lock();
                    try {
                        listing.removeAll(inProcess);
                        if (!keepingSourceFile) {
                            listing.removeAll(recentlyProcessed);
                        }

                        fileQueue.clear();
                        fileQueue.addAll(listing);

                        queueLastUpdated.set(System.currentTimeMillis());
                        recentlyProcessed.clear();

                        if (listing.isEmpty()) {
                            context.yield();
                        }
                    } finally {
                        queueLock.unlock();
                    }
                } finally {
                    listingLock.unlock();
                }
            }
        }

        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final List<File> files = new ArrayList<>(batchSize);
        queueLock.lock();
        try {
            fileQueue.drainTo(files, batchSize);
            if (files.isEmpty()) {
                return;
            } else {
                inProcess.addAll(files);
            }
        } finally {
            queueLock.unlock();
        }

        final ListIterator<File> itr = files.listIterator();
        FlowFile flowFile = null;
        try {
            final Path directoryPath = directory.toPath();
            while (itr.hasNext()) {
                //??????zip???
                final File file1 = itr.next();
                File file = exportJson(file1,targetSrid);
                final Path filePath = file.toPath();
                //????????????
                final Path relativePath = directoryPath.relativize(filePath.getParent());
                String relativePathString = relativePath.toString() + "/";
                if (relativePathString.isEmpty()) {
                    relativePathString = "./";
                }
                //????????????
                final Path absPath = filePath.toAbsolutePath();
                final String absPathString = absPath.getParent().toString() + "/";

                flowFile = session.create();
                final long importStart = System.nanoTime();
                flowFile = session.importFrom(filePath, keepingSourceFile, flowFile);
                final long importNanos = System.nanoTime() - importStart;
                final long importMillis = TimeUnit.MILLISECONDS.convert(importNanos, TimeUnit.NANOSECONDS);

                flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), file.getName());
                flowFile = session.putAttribute(flowFile, CoreAttributes.PATH.key(), relativePathString);
                flowFile = session.putAttribute(flowFile, CoreAttributes.ABSOLUTE_PATH.key(), absPathString);
                Map<String, String> attributes = getAttributesFromFile(filePath);
                if (attributes.size() > 0) {
                    flowFile = session.putAllAttributes(flowFile, attributes);
                }

                session.getProvenanceReporter().receive(flowFile, file.toURI().toString(), importMillis);
                session.transfer(flowFile, REL_SUCCESS);
                logger.info("added {} to flow", new Object[]{flowFile});

                if (!isScheduled()) {  // if processor stopped, put the rest of the files back on the queue.
                    queueLock.lock();
                    try {
                        while (itr.hasNext()) {
                            final File nextFile = itr.next();
                            fileQueue.add(nextFile);
                            inProcess.remove(nextFile);
                        }
                    } finally {
                        queueLock.unlock();
                    }
                }
            }
            session.commit();
        } catch (final Exception e) {
            logger.error("Failed to retrieve files due to {}", e);

            // anything that we've not already processed needs to be put back on the queue
            if (flowFile != null) {
                session.remove(flowFile);
            }
        } finally {
            queueLock.lock();
            try {
                inProcess.removeAll(files);
                recentlyProcessed.addAll(files);
            } finally {
                queueLock.unlock();
            }
        }
    }
    public static String unZipFiles(File zipFile, String descDir) throws IOException {
        File pathFile = new File(descDir);
        if (!pathFile.exists()) {
            pathFile.mkdirs();
        }
        //??????zip??????????????????????????????????????????
        ZipFile zip = new ZipFile(zipFile, Charset.forName("GBK"));
        ArrayList<Object> list = new ArrayList<>();
        for (Enumeration entries = zip.entries(); entries.hasMoreElements(); ) {
            ZipEntry entry = (ZipEntry) entries.nextElement();
            String zipEntryName = entry.getName();
            InputStream in = zip.getInputStream(entry);
            String outPath = (descDir +"/"+ zipEntryName).replaceAll("\\*", "/");
            //????????????????????????,??????????????????????????????
            File file = new File(outPath.substring(0, outPath.lastIndexOf('/')));
            if (!file.exists()) {
                file.mkdirs();
            }
            //???????????????????????????????????????,???????????????????????????,???????????????
            if (new File(outPath).isDirectory()) {
                continue;
            }
            //????????????????????????
            list.add(outPath);
           // System.out.println(outPath);
            OutputStream out = new FileOutputStream(outPath);

            byte[] buf1 = new byte[1024];
            int len;
            while ((len = in.read(buf1)) > 0) {
                out.write(buf1, 0, len);
            }
            in.close();
            out.close();
        }
        return list.get(0).toString();
    }
    public static File exportJson(File zipFile ,int targetSrid) throws IOException, FactoryException {
        String encode = "GBK";
        String tmp = "/opt/tmp/";
       // String tmp = "D:\\tmp\\";
        File jsonFile=null;
        String shipPath = unZipFiles(zipFile, tmp);
        File shapefile = new File(shipPath);
        GeoToolsShapefileDataStore geoToolsShapefileDataStore = new GeoToolsShapefileDataStore(shapefile.toURI().toURL());
        geoToolsShapefileDataStore.getShapefileDataStore().setCharset(Charset.forName(encode));
        CoordinateReferenceSystem crs = geoToolsShapefileDataStore.getShapefileDataStore().getSchema().getCoordinateReferenceSystem();
        Integer srid = null;
        if (null != crs) {
            srid = CRS.lookupEpsgCode(crs, true);
        }
        FeatureCollection featureCollection = geoToolsShapefileDataStore.getShapefileDataStore().getFeatureSource().getFeatures();

        GeoTransform geoTransform = new GeoTransform(srid, targetSrid);
        long time = System.currentTimeMillis();
        try (FeatureIterator iterator = featureCollection.features()) {
            int index = 0;
            String json="[";
            while (iterator.hasNext()) {
                Feature feature = iterator.next();
                Iterator<Property> propertyIterator = feature.getProperties().iterator();
                json+="{";
                while (propertyIterator.hasNext()) {
                    Property property = propertyIterator.next();
                    String typeName = property.getType().getName().toString().toUpperCase();
                    if(property.getType().getBinding().getName().startsWith("org.locationtech.jts.geom")){
                        Geometry geometry =(Geometry)  property.getValue();
                        Geometry transform = geoTransform.transform(geometry);
                        transform.setSRID(targetSrid);
                        String wkb = WKBWriter.toHex(new WKBWriter(2, true).write(transform));
                        json+="\"geom\""+":"+"\""+wkb+"\",";
                    }else {
                        Class<?> aClass = property.getValue().getClass();
                        if(aClass.toString().equals("class java.lang.String")){
                            json+="\""+typeName+"\""+":"+"\""+property.getValue()+"\",";
                        }else {json+="\""+typeName+"\""+":"+property.getValue()+",";}
                    }

                    /*switch (typeName) {
                        case "GEOMETRY":
                        case "POINT":
                        case "MULTIPOINT":
                        case "LINESTRING":
                        case "MULTILINESTRING":
                        case "POLYGON":
                        case "MULTIPOLYGON":
                                  *//*  Geometry geometry = (Geometry) property.getValue();
                                    geometry.setSRID(srid);
                                    dataRows.add(WKBWriter.toHex(new WKBWriter(2, true).write(geometry)));*//*
                            Geometry geometry =(Geometry)  property.getValue();
                            Geometry transform = geoTransform.transform(geometry);
                            json+="\"geom\""+":"+"\""+property.getValue()+"\",";
                            break;
                        default:
                            Class<?> aClass = property.getValue().getClass();
                            if(aClass.toString().equals("class java.lang.String")){
                                json+="\""+typeName+"\""+":"+"\""+property.getValue()+"\",";
                            }else {json+="\""+typeName+"\""+":"+property.getValue()+",";}
                            break;
                    }*/
                }
                json = json.substring(0, json.length() - 1);
                json+="},";
                index++;
            }
           // System.out.println((json.substring(0, json.length() -1))+"]");
            String fileName = zipFile.toPath().getFileName().toString().replace("zip","json");
            String fileJson = tmp+"/"+fileName;
             jsonFile = new File(fileJson);
            if (!jsonFile.exists()) {
                if (!jsonFile.getParentFile().exists()) {
                    jsonFile.getParentFile().mkdirs();
                }

                jsonFile.createNewFile();
            }
            BufferedWriter printer = new BufferedWriter(new FileWriter(jsonFile), 5242);
            printer.write((json.substring(0, json.length() -1))+"]");
            printer.flush();
            printer.close();
        }

        System.out.println("????????????");
        System.out.println(System.currentTimeMillis() - time);
        return jsonFile;
    }

}
