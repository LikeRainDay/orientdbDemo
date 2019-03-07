package com.orientdb.improt

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDBConfig
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.record.impl.OVertexDocument
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationListener
import org.springframework.util.StringUtils
import java.io.BufferedReader
import java.io.File
import java.io.FileInputStream
import java.io.InputStreamReader


/**
 * describe: 导入操作
 * author 候帅
 * date 2019-03-06
 */
class ImportListener : ApplicationListener<ApplicationReadyEvent> {

    private val log = LoggerFactory.getLogger(ImportListener::class.java)

    override fun onApplicationEvent(event: ApplicationReadyEvent) {
        val orientDB = OrientDB("remote:localhost", OrientDBConfig.defaultConfig())
//        val orientDB = OrientDB("remote:114.116.83.50", OrientDBConfig.defaultConfig())
        val db = orientDB.open("pgap_test", "root", "haizhi1234")
        importVer(db)
        importEdge(db)
        db.close()
        orientDB.close()
    }

    private fun importEdge(db: ODatabaseSession) {
        if (db.getClass("stf_edge") == null)
            db.createEdgeClass("stf_edge")

        readFileFromResources("/data/soc-pokec-relationships.txt") {
            val addEdge = findVertexById(it[0], db)
            addEdge?.addEdge(findVertexById(it[1], db))
            addEdge?.save<OVertexDocument>()
            log.info("----edge is ---${it[0]} to ${it[1]}")
        }
    }

    private fun importVer(db: ODatabaseSession) {
        var stfVe = db.getClass("stf")
        if (stfVe == null)
            stfVe = db.createVertexClass("stf")
        if (stfVe.getProperty("name") == null) {
            stfVe.createProperty("name", OType.STRING)
            stfVe.createIndex("stf_name_index", OClass.INDEX_TYPE.NOTUNIQUE, "name")
        }
        if (stfVe.getProperty("id") == null) {
            stfVe.createProperty("id", OType.STRING)
            stfVe.createIndex("stf_id_index", OClass.INDEX_TYPE.UNIQUE, "id")
        }
        if (stfVe.getProperty("createTime") == null) {
            stfVe.createProperty("createTime", OType.STRING)
        }
        if (stfVe.getProperty("endTime") == null) {
            stfVe.createProperty("endTime", OType.STRING)
        }
        if (stfVe.getProperty("type") == null) {
            stfVe.createProperty("type", OType.STRING)
        }

        readFileFromResources("/data/soc-pokec-profiles.txt") {
            val vAddress = db.newVertex("stf")
            // 创建顶点
            vAddress.setProperty("id", it[0])
            vAddress.setProperty("name", it[4])
            vAddress.setProperty("createTime", it[5])
            vAddress.setProperty("endTime", it[6])
            vAddress.setProperty("type", it[7])
            vAddress.save<OVertex>()
            log.info("----v is ---${it[0]}")
        }
    }

    private fun readFileFromResources(path: String, apply: (List<String>) -> Unit) {
//        val resourceAsStream = Thread.currentThread().contextClassLoader.getResourceAsStream(path)
        val file = File(path)
        // 读取
        val bufferedReader = BufferedReader(InputStreamReader( FileInputStream(file)))
        while (true) {
            val readLine = bufferedReader.readLine()
            if (StringUtils.isEmpty(readLine))
                break
            val split = readLine.split('\t')
            apply.invoke(split)
        }
    }

    fun findVertexById(id: String, db: ODatabaseSession): OVertex? {
        val query = "select * from `stf` where id = ?"
        val findFirst = db.query(query, id).vertexStream().findFirst()
        if (!findFirst.isPresent) {
            return null
        }
        return findFirst.get()
    }
}