package study.specific

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase

/**
 * 비교 테스트용 자바 [org.apache.avro.specific.SpecificRecord].
 * avro4k가 만드는 `customerManagement.avro.Customer`와 풀네임이 겹치지 않게 `study.specific`에 둔다.
 * 필드 순서와 타입은 그 스키마와 같다.
 */
class Customer() : SpecificRecordBase() {
    private var id: Int = 0
    private var name: String = ""
    private var email: String? = null

    constructor(id: Int, name: String, email: String?) : this() {
        this.id = id
        this.name = name
        this.email = email
    }

    fun getId(): Int = id

    fun getName(): String = name

    fun getEmail(): String? = email

    override fun getSchema(): Schema = `SCHEMA$`

    override fun get(field: Int): Any? = when (field) {
        0 -> id
        1 -> name
        2 -> email
        else -> error("bad index $field")
    }

    override fun put(field: Int, value: Any?) {
        when (field) {
            0 -> id = (value as Number).toInt()
            1 -> name = value.toString()
            2 -> email = value?.toString()
            else -> error("bad index $field")
        }
    }

    companion object {
        // Avro가 스키마 풀네임으로 이 클래스를 찾을 때 보는 정적 필드다.
        @JvmField
        val `SCHEMA$`: Schema = parser().parse(SCHEMA_JSON)

        private const val SCHEMA_JSON = """
            {"namespace":"study.specific","type":"record","name":"Customer",
             "fields":[
               {"name":"id","type":"int"},
               {"name":"name","type":"string"},
               {"name":"email","type":["null","string"],"default":null}
             ]}
            """

        private fun parser(): Schema.Parser = Schema.Parser()

        @JvmStatic
        fun getClassSchema(): Schema = `SCHEMA$`
    }
}
