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
package org.apache.nifi.toolkit.admin.nodemanager

import org.apache.commons.cli.ParseException
import org.apache.nifi.toolkit.admin.client.ClientFactory
import org.apache.nifi.util.NiFiProperties
import org.apache.nifi.web.api.dto.ClusterDTO
import org.apache.nifi.web.api.dto.NodeDTO
import org.apache.nifi.web.api.entity.ClusterEntity
import org.apache.nifi.web.api.entity.NodeEntity
import org.junit.Rule
import org.junit.contrib.java.lang.system.ExpectedSystemExit
import org.junit.contrib.java.lang.system.SystemOutRule
import spock.lang.Specification

import javax.ws.rs.client.Client
import javax.ws.rs.client.Invocation
import javax.ws.rs.client.WebTarget
import javax.ws.rs.core.Response

class NodeManagerToolSpec extends Specification{

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none()

    @Rule
    public final SystemOutRule systemOutRule = new SystemOutRule().enableLog()


    def "print help and usage info"() {

        given:
        def ClientFactory clientFactory = Mock ClientFactory
        def config = new NodeManagerTool()

        when:
        config.parse(clientFactory,["-h"] as String[])

        then:
        systemOutRule.getLog().contains("usage: org.apache.nifi.toolkit.admin.nodemanager.NodeManagerTool")
    }

    def "throws exception missing bootstrap conf flag"() {

        given:
        def ClientFactory clientFactory = Mock ClientFactory
        def config = new NodeManagerTool()

        when:
        config.parse(clientFactory,["-d", "/install/nifi"] as String[])

        then:
        def e = thrown(ParseException)
        e.message == "Missing -b option"
    }

    def "throws exception missing directory"(){

        given:
        def ClientFactory clientFactory = Mock ClientFactory
        def config = new NodeManagerTool()

        when:
        config.parse(clientFactory,["-b","src/test/resources/notify/conf/bootstrap.conf"] as String[])

        then:
        def e = thrown(ParseException)
        e.message == "Missing -d option"
    }

    def "throws exception missing operation"(){

        given:
        def ClientFactory clientFactory = Mock ClientFactory
        def config = new NodeManagerTool()

        when:
        config.parse(clientFactory,["-b","src/test/resources/notify/conf/bootstrap.conf","-d", "/install/nifi"] as String[])

        then:
        def e = thrown(ParseException)
        e.message == "Missing -o option"
    }

    def "throws exception invalid operation"(){

        given:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def ClientFactory clientFactory = Mock ClientFactory
        def Client client = Mock Client
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def ClusterEntity clusterEntity = Mock ClusterEntity
        def ClusterDTO clusterDTO = Mock ClusterDTO
        def NodeDTO nodeDTO = new NodeDTO()
        nodeDTO.address = "localhost"
        nodeDTO.nodeId = "1"
        nodeDTO.status = "CONNECTED"
        nodeDTO.apiPort = 8080
        def List<NodeDTO> nodeDTOs = [nodeDTO]
        def NodeEntity nodeEntity = new NodeEntity()
        nodeEntity.node = nodeDTO
        def config = new NodeManagerTool()


        niFiProperties.getProperty(_) >> "localhost"
        clientFactory.getClient(_,_) >> client
        client.target(_ as String) >> resource
        resource.request() >> builder
        builder.get() >> response
        builder.put(_,_) >> response
        builder.delete() >> response
        response.getStatus() >> 200
        response.readEntity(ClusterEntity.class) >> clusterEntity
        response.readEntity(NodeEntity.class) >> nodeEntity
        clusterEntity.getCluster() >> clusterDTO
        clusterDTO.getNodes() >> nodeDTOs
        nodeDTO.address >> "localhost"

        when:
        config.parse(clientFactory,["-b","src/test/resources/notify/conf/bootstrap.conf","-d","/install/nifi","-o","fake"] as String[])

        then:
        def e = thrown(ParseException)
        e.message == "Invalid operation provided: fake"
    }


    def "get node info successfully"(){

        given:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def ClusterEntity clusterEntity = Mock ClusterEntity
        def ClusterDTO clusterDTO = Mock ClusterDTO
        def NodeDTO nodeDTO = new NodeDTO()
        nodeDTO.address = "1"
        def List<NodeDTO> nodeDTOs = [nodeDTO]
        def config = new NodeManagerTool()

        when:
        def entity = config.getCurrentNode(clusterEntity,niFiProperties)

        then:

        1 * clusterEntity.getCluster() >> clusterDTO
        1 * clusterDTO.getNodes() >> nodeDTOs
        2 * niFiProperties.getProperty(_) >> "1"
        entity == nodeDTO

    }


    def "delete node successfully"(){

        given:
        def String url = "http://locahost:8080/nifi-api/controller"
        def Client client = Mock Client
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def config = new NodeManagerTool()

        when:
        config.deleteNode(url,client,null)

        then:

        1 * client.target(_ as String) >> resource
        1 * resource.request() >> builder
        1 * builder.delete() >> response
        1 * response.getStatus() >> 200

    }

    def "delete secured node successfully"(){

        given:
        def String url = "https://locahost:8080/nifi-api/controller"
        def Client client = Mock Client
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def config = new NodeManagerTool()

        when:
        config.deleteNode(url,client,null)

        then:

        1 * client.target(_ as String) >> resource
        1 * resource.request() >> builder
        1 * builder.header(_,_) >> builder
        1 * builder.delete() >> response
        1 * response.getStatus() >> 200

    }


    def "delete node failed"(){

        given:
        def String url = "http://locahost:8080/nifi-api/controller"
        def Client client = Mock Client
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def Response.StatusType statusType = Mock Response.StatusType
        def config = new NodeManagerTool()

        when:
        config.deleteNode(url,client,null)

        then:
        1 * client.target(_ as String) >> resource
        1 * resource.request() >> builder
        1 * builder.delete() >> response
        2 * response.getStatus() >> 403
        1 * response.readEntity(String.class) >> "Unauthorized User"
        def e = thrown(RuntimeException)
        e.message == "Failed with HTTP error code 403 with reason: Unauthorized User"

    }

    def "update node successfully"(){

        given:
        def String url = "http://locahost:8080/nifi-api/controller"
        def Client client = Mock Client
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def NodeDTO nodeDTO = new NodeDTO()
        def NodeEntity nodeEntity = Mock NodeEntity
        def config = new NodeManagerTool()

        when:
        def entity = config.updateNode(url,client,nodeDTO,NodeManagerTool.STATUS.DISCONNECTING,null)

        then:
        1 * client.target(_ as String) >> resource
        1 * resource.request() >> builder
        1 * builder.put(_) >> response
        1 * response.getStatus() >> 200
        1 * response.readEntity(NodeEntity.class) >> nodeEntity
        entity == nodeEntity

    }

    def "update secured node successfully"(){

        given:
        def String url = "https://locahost:8080/nifi-api/controller"
        def Client client = Mock Client
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def NodeDTO nodeDTO = new NodeDTO()
        def NodeEntity nodeEntity = Mock NodeEntity
        def config = new NodeManagerTool()

        when:
        def entity = config.updateNode(url,client,nodeDTO,NodeManagerTool.STATUS.DISCONNECTING,null)

        then:
        1 * client.target(_ as String) >> resource
        1 * resource.request() >> builder
        1 * builder.header(_,_) >> builder
        1 * builder.put(_) >> response
        1 * response.getStatus() >> 200
        1 * response.readEntity(NodeEntity.class) >> nodeEntity
        entity == nodeEntity

    }

    def "update node fails"(){

        given:
        def String url = "http://locahost:8080/nifi-api/controller"
        def Client client = Mock Client
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def Response.StatusType statusType = Mock Response.StatusType
        def NodeDTO nodeDTO = new NodeDTO()
        def config = new NodeManagerTool()

        when:
        config.updateNode(url,client,nodeDTO,NodeManagerTool.STATUS.DISCONNECTING,null)

        then:
        1 * client.target(_ as String) >> resource
        1 * resource.request() >> builder
        1 * builder.put(_) >> response
        2 * response.getStatus() >> 403
        1 * response.readEntity(String.class) >> "Unauthorized User"
        def e = thrown(RuntimeException)
        e.message == "Failed with HTTP error code 403 with reason: Unauthorized User"

    }

    def "get node status successfully"(){

        given:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def Client client = Mock Client
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def config = new NodeManagerTool()

        when:
        config.getStatus(client,niFiProperties,null)

        then:
        niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_PORT) >> "8080"
        niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_HOST) >> "localhost"
        1 * client.target(_ as String) >> resource
        1 * resource.request() >> builder
        1 * builder.get() >> response
        1 * response.getStatus() >> 200
    }

    def "get node status when unavailable"(){

        given:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def Client client = Mock Client
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def config = new NodeManagerTool()

        when:
        config.getStatus(client,niFiProperties,null)

        then:
        niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_PORT) >> "8080"
        niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_HOST) >> "localhost"
        1 * client.target(_ as String) >> resource
        1 * resource.request() >> builder
        1 * builder.get() >> response
        2 * response.getStatus() >> 403
        1 * response.readEntity(String.class) >> "Unauthorized User"
    }

    def "get multiple node status successfully"(){

        given:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def Client client = Mock Client
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def config = new NodeManagerTool()
        def activeUrls = ["https://localhost:8080","https://localhost1:8080"]

        when:
        config.getStatus(client,niFiProperties,activeUrls)

        then:
        niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_PORT) >> "8080"
        niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_HOST) >> "localhost"
        2 * client.target(_ as String) >> resource
        2 * resource.request() >> builder
        2 * builder.get() >> response
        2 * response.getStatus() >> 200
    }


    def "disconnect node successfully"(){

        setup:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def Client client = Mock Client
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def ClusterEntity clusterEntity = Mock ClusterEntity
        def ClusterDTO clusterDTO = Mock ClusterDTO
        def NodeDTO nodeDTO = new NodeDTO()
        nodeDTO.address = "localhost"
        nodeDTO.nodeId = "1"
        nodeDTO.status = "CONNECTED"
        def List<NodeDTO> nodeDTOs = [nodeDTO]
        def NodeEntity nodeEntity = new NodeEntity()
        nodeEntity.node = nodeDTO
        def config = new NodeManagerTool()


        niFiProperties.getProperty(_) >> "localhost"
        client.target(_ as String) >> resource
        resource.request() >> builder
        builder.get() >> response
        builder.put(_) >> response
        response.getStatus() >> 200
        response.readEntity(ClusterEntity.class) >> clusterEntity
        response.readEntity(NodeEntity.class) >> nodeEntity
        clusterEntity.getCluster() >> clusterDTO
        clusterDTO.getNodes() >> nodeDTOs
        nodeDTO.address >> "localhost"

        expect:
        config.disconnectNode(client, niFiProperties,["http://localhost:8080"],null)

    }


    def "disconnect secured node successfully"(){

        setup:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def Client client = Mock Client
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def ClusterEntity clusterEntity = Mock ClusterEntity
        def ClusterDTO clusterDTO = Mock ClusterDTO
        def NodeDTO nodeDTO = new NodeDTO()
        nodeDTO.address = "localhost"
        nodeDTO.nodeId = "1"
        nodeDTO.status = "CONNECTED"
        def List<NodeDTO> nodeDTOs = [nodeDTO]
        def NodeEntity nodeEntity = new NodeEntity()
        nodeEntity.node = nodeDTO
        def config = new NodeManagerTool()


        niFiProperties.getProperty(_) >> "localhost"
        client.target(_ as String) >> resource
        resource.request() >> builder
        builder.get() >> response
        builder.header(_,_) >> builder
        builder.put(_) >> response
        response.getStatus() >> 200
        response.readEntity(ClusterEntity.class) >> clusterEntity
        response.readEntity(NodeEntity.class) >> nodeEntity
        clusterEntity.getCluster() >> clusterDTO
        clusterDTO.getNodes() >> nodeDTOs
        nodeDTO.address >> "localhost"

        expect:
        config.disconnectNode(client, niFiProperties,["https://localhost:8080"],null)

    }
    def "connect node successfully"(){

        setup:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def Client client = Mock Client
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def ClusterEntity clusterEntity = Mock ClusterEntity
        def ClusterDTO clusterDTO = Mock ClusterDTO
        def NodeDTO nodeDTO = new NodeDTO()
        nodeDTO.address = "localhost"
        nodeDTO.nodeId = "1"
        nodeDTO.status = "DISCONNECTED"
        def List<NodeDTO> nodeDTOs = [nodeDTO]
        def NodeEntity nodeEntity = new NodeEntity()
        nodeEntity.node = nodeDTO
        def config = new NodeManagerTool()


        niFiProperties.getProperty(_) >> "localhost"
        client.target(_ as String) >> resource
        resource.request() >> builder
        builder.get() >> response
        builder.put(_) >> response
        response.getStatus() >> 200
        response.readEntity(ClusterEntity.class) >> clusterEntity
        response.readEntity(NodeEntity.class) >> nodeEntity
        clusterEntity.getCluster() >> clusterDTO
        clusterDTO.getNodes() >> nodeDTOs
        nodeDTO.address >> "localhost"

        expect:
        config.connectNode(client, niFiProperties,["http://localhost:8080"],null)

    }

    def "remove node successfully"(){

        setup:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def Client client = Mock Client
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def ClusterEntity clusterEntity = Mock ClusterEntity
        def ClusterDTO clusterDTO = Mock ClusterDTO
        def NodeDTO nodeDTO = new NodeDTO()
        nodeDTO.address = "localhost"
        nodeDTO.nodeId = "1"
        nodeDTO.status = "CONNECTED"
        def List<NodeDTO> nodeDTOs = [nodeDTO]
        def NodeEntity nodeEntity = new NodeEntity()
        nodeEntity.node = nodeDTO
        def config = new NodeManagerTool()


        niFiProperties.getProperty(_) >> "localhost"
        client.target(_ as String) >> resource
        resource.request() >> builder
        builder.get() >> response
        builder.put(_) >> response
        builder.delete() >> response
        response.getStatus() >> 200
        response.readEntity(ClusterEntity.class) >> clusterEntity
        response.readEntity(NodeEntity.class) >> nodeEntity
        clusterEntity.getCluster() >> clusterDTO
        clusterDTO.getNodes() >> nodeDTOs
        nodeDTO.address >> "localhost"

        expect:
        config.removeNode(client, niFiProperties,["http://localhost:8080"],null)

    }

    def "parse args and delete node"(){

        setup:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def ClientFactory clientFactory = Mock ClientFactory
        def Client client = Mock Client
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def ClusterEntity clusterEntity = Mock ClusterEntity
        def ClusterDTO clusterDTO = Mock ClusterDTO
        def NodeDTO nodeDTO = new NodeDTO()
        nodeDTO.address = "localhost"
        nodeDTO.nodeId = "1"
        nodeDTO.status = "CONNECTED"
        def List<NodeDTO> nodeDTOs = [nodeDTO]
        def NodeEntity nodeEntity = new NodeEntity()
        nodeEntity.node = nodeDTO
        def config = new NodeManagerTool()


        niFiProperties.getProperty(_) >> "localhost"
        clientFactory.getClient(_,_) >> client
        client.target(_ as String) >> resource
        resource.request() >> builder
        builder.get() >> response
        builder.put(_) >> response
        builder.delete() >> response
        response.getStatus() >> 200
        response.readEntity(ClusterEntity.class) >> clusterEntity
        response.readEntity(NodeEntity.class) >> nodeEntity
        clusterEntity.getCluster() >> clusterDTO
        clusterDTO.getNodes() >> nodeDTOs
        nodeDTO.address >> "localhost"


        expect:
        config.parse(clientFactory,["-b","src/test/resources/notify/conf/bootstrap.conf","-d","/bogus/nifi/dir","-o","remove","-u","http://localhost:8080,http://localhost1:8080"] as String[])

    }

    def "parse args and fail connecting secured node"(){

        setup:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def ClientFactory clientFactory = Mock ClientFactory
        def Client client = Mock Client
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def ClusterEntity clusterEntity = Mock ClusterEntity
        def ClusterDTO clusterDTO = Mock ClusterDTO
        def NodeDTO nodeDTO = new NodeDTO()
        nodeDTO.address = "localhost"
        nodeDTO.nodeId = "1"
        nodeDTO.status = "DISCONNECTED"
        def List<NodeDTO> nodeDTOs = [nodeDTO]
        def NodeEntity nodeEntity = new NodeEntity()
        nodeEntity.node = nodeDTO
        def config = new NodeManagerTool()


        niFiProperties.getProperty(_) >> "localhost"
        clientFactory.getClient(_,_) >> client
        client.target(_ as String) >> resource
        resource.request() >> builder
        builder.get() >> response
        builder.header(_,_) >> builder
        builder.put(_) >> response
        response.getStatus() >> 200
        response.readEntity(ClusterEntity.class) >> clusterEntity
        response.readEntity(NodeEntity.class) >> nodeEntity
        clusterEntity.getCluster() >> clusterDTO
        clusterDTO.getNodes() >> nodeDTOs
        nodeDTO.address >> "localhost"

        when:
        config.parse(clientFactory,["-b","src/test/resources/notify/conf_secure/bootstrap.conf","-d","/bogus/nifi/dir","-o","connect","-u","https://localhost:8080,https://localhost1:8080"] as String[])

        then:
        def e = thrown(UnsupportedOperationException)
        e.message == "Proxy DN is required for sending a notification to this node or cluster"


    }

    def "parse args and connect secured node"(){

        setup:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def ClientFactory clientFactory = Mock ClientFactory
        def Client client = Mock Client
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def ClusterEntity clusterEntity = Mock ClusterEntity
        def ClusterDTO clusterDTO = Mock ClusterDTO
        def NodeDTO nodeDTO = new NodeDTO()
        nodeDTO.address = "localhost"
        nodeDTO.nodeId = "1"
        nodeDTO.status = "DISCONNECTED"
        def List<NodeDTO> nodeDTOs = [nodeDTO]
        def NodeEntity nodeEntity = new NodeEntity()
        nodeEntity.node = nodeDTO
        def config = new NodeManagerTool()


        niFiProperties.getProperty(_) >> "localhost"
        clientFactory.getClient(_,_) >> client
        client.target(_ as String) >> resource
        resource.request() >> builder
        builder.get() >> response
        builder.header(_,_) >> builder
        builder.put(_) >> response
        response.getStatus() >> 200
        response.readEntity(ClusterEntity.class) >> clusterEntity
        response.readEntity(NodeEntity.class) >> nodeEntity
        clusterEntity.getCluster() >> clusterDTO
        clusterDTO.getNodes() >> nodeDTOs
        nodeDTO.address >> "localhost"

        expect:
        config.parse(clientFactory,["-b","src/test/resources/notify/conf_secure/bootstrap.conf","-d","/bogus/nifi/dir","-o","connect","-u","https://localhost:8080,https://localhost1:8080","-p","ydavis@nifi"] as String[])

    }

    def "parse args and disconnect secured node"(){

        setup:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def ClientFactory clientFactory = Mock ClientFactory
        def Client client = Mock Client
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def ClusterEntity clusterEntity = Mock ClusterEntity
        def ClusterDTO clusterDTO = Mock ClusterDTO
        def NodeDTO nodeDTO = new NodeDTO()
        nodeDTO.address = "localhost"
        nodeDTO.nodeId = "1"
        nodeDTO.status = "CONNECTED"
        def List<NodeDTO> nodeDTOs = [nodeDTO]
        def NodeEntity nodeEntity = new NodeEntity()
        nodeEntity.node = nodeDTO
        def config = new NodeManagerTool()

        niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_PORT) >> "8081"
        niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_HOST) >> "localhost"
        clientFactory.getClient(_,_) >> client
        client.target(_ as String) >> resource
        resource.request() >> builder
        builder.get() >> response
        builder.header(_,_) >> builder
        builder.put(_) >> response
        response.getStatus() >> 200
        response.readEntity(ClusterEntity.class) >> clusterEntity
        response.readEntity(NodeEntity.class) >> nodeEntity
        clusterEntity.getCluster() >> clusterDTO
        clusterDTO.getNodes() >> nodeDTOs
        nodeDTO.address >> "localhost"

        expect:
        config.parse(clientFactory,["-b","src/test/resources/notify/conf_secure/bootstrap.conf","-d","/bogus/nifi/dir","-o","disconnect","-u","https://localhost:8080,https://localhost1:8080","-p","ydavis@nifi"] as String[])

    }

    def "parse args and delete secured node"(){

        setup:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def ClientFactory clientFactory = Mock ClientFactory
        def Client client = Mock Client
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def ClusterEntity clusterEntity = Mock ClusterEntity
        def ClusterDTO clusterDTO = Mock ClusterDTO
        def NodeDTO nodeDTO = new NodeDTO()
        nodeDTO.address = "localhost"
        nodeDTO.nodeId = "1"
        nodeDTO.status = "CONNECTED"
        def List<NodeDTO> nodeDTOs = [nodeDTO]
        def NodeEntity nodeEntity = new NodeEntity()
        nodeEntity.node = nodeDTO
        def config = new NodeManagerTool()


        niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_PORT) >> "8081"
        niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_HOST) >> "localhost"
        clientFactory.getClient(_,_) >> client
        client.target(_ as String) >> resource
        resource.request() >> builder
        builder.get() >> response
        builder.header(_,_) >> builder
        builder.put(_) >> response
        builder.delete() >> response
        response.getStatus() >> 200
        response.readEntity(ClusterEntity.class) >> clusterEntity
        response.readEntity(NodeEntity.class) >> nodeEntity
        clusterEntity.getCluster() >> clusterDTO
        clusterDTO.getNodes() >> nodeDTOs
        nodeDTO.address >> "localhost"

        expect:
        config.parse(clientFactory,["-b","src/test/resources/notify/conf_secure/bootstrap.conf","-d","/bogus/nifi/dir","-o","remove","-u","https://localhost:8080,https://localhost1:8080","-p","ydavis@nifi"] as String[])

    }


}
