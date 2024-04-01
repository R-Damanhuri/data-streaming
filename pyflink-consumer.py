from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.connectors.jdbc import JdbcExecutionOptions, JdbcConnectionOptions, JdbcSink
from pyflink.common.types import Row
from pyflink.common.typeinfo import Types
import json
from datetime import datetime
from pyflink.datastream.connectors.elasticsearch import Elasticsearch7SinkBuilder, ElasticsearchEmitter

# Parsing string JSON menjadi objek Python dan konversi ke tipe data Row Flink
def parse_json(json_str):
    json_obj = json.loads(json_str)

    # Konversi transactionDate ke format datetime
    json_obj['transactionDate'] = datetime.strptime(json_obj['transactionDate'], '%Y-%m-%d %H:%M:%S')
    return Row(
        transactionId=json_obj['transactionId'],
        productId=json_obj['productId'],
        productName=json_obj['productName'],
        productCategory=json_obj['productCategory'],
        productPrice=json_obj['productPrice'],
        productQuantity=json_obj['productQuantity'],
        productBrand=json_obj['productBrand'],
        customerId=json_obj['customerId'],
        transactionDate=json_obj['transactionDate'],
        paymentMethod=json_obj['paymentMethod'],
        totalAmount=json_obj['totalAmount']
    )

# Process data stream untuk menghasilkan sales per category
def per_category(row):
    return Row(
        transactionDate = row['transactionDate'],
        category = row['productCategory'],
        totalSales = row['totalAmount']
    )

# Process data stream untuk menghasilkan sales per day
def per_day(row):
    return Row(
        transactionDate = row['transactionDate'],
        totalSales = row['totalAmount']
    )

# Process data stream untuk menghasilkan sales per month
def per_month(row):
    return Row(
        transactionDate = row['transactionDate'],
        year = row['transactionDate'].year,
        month= row['transactionDate'].month,
        totalSales = row['totalAmount']
    )

# Format data stream dari Row menjadi Map
def to_map(row):
    row['transactionDate'] = row['transactionDate'].strftime('%Y-%m-%d %H:%M:%S')
    return {
        'key': row['transactionId'],
        'json': json.dumps(row.as_dict())
    }

def read_from_kafka():

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(
        "file:///D:/Informatika/Eksplorasi/Data-Engineering/data-streaming/flink-sql-connector-kafka-3.1.0-1.17.jar",
        "file:///D:/Informatika/Eksplorasi/Data-Engineering/data-streaming/flink-connector-jdbc-3.1.2-1.17.jar",
        "file:///D:/Informatika/Eksplorasi/Data-Engineering/data-streaming/postgresql-42.7.3.jar",
        "file:///D:/Informatika/Eksplorasi/Data-Engineering/data-streaming/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar"   
    )
    
    print("-----Environment Created-----")
    
    properties = {
        'bootstrap.servers': 'localhost:9092',
    }

    kafka_source = KafkaSource.builder() \
        .set_topics('financial_transactions') \
        .set_properties(properties) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Create DataStream dari Kafka source
    transaction_stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")   
    print("-----Kafka Source Created-----")


    """
    INSERT INTO TABLE
    Note:   Table telah dibuat dahulu di pgAdmin4 (postgres) dengan Create Tables.sql
    """
    # Definisi tipe data pada Flink
    type_info = Types.ROW_NAMED(
        ["transactionId","productId","productName", "productCategory", "productPrice", "productQuantity",
         "productBrand", "customerId", "transactionDate", "paymentMethod", "totalAmount"],
        [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.INT(),      
        Types.STRING(), Types.STRING(), Types.SQL_TIMESTAMP(), Types.STRING(), Types.DOUBLE()]
        )
    
    # Parsing data string JSON yang diterima Flink dari Kafka, output RowTypeInfo
    transaction_stream = transaction_stream.map(lambda x: parse_json(x), output_type=type_info)

    # Postgres Connection
    jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
    username = "postgres"
    password = "postgres"

    execOptions = JdbcExecutionOptions.builder() \
        .with_batch_size(1000) \
        .with_batch_interval_ms(200) \
        .with_max_retries(5) \
        .build()
    
    connOptions = JdbcConnectionOptions.JdbcConnectionOptionsBuilder() \
        .with_url(jdbcUrl) \
        .with_driver_name("org.postgresql.Driver") \
        .with_user_name(username) \
        .with_password(password) \
        .build()

    # Insert Into transactions Table Sink
    insert_transactions = JdbcSink.sink(
        "INSERT INTO transactions(transaction_id, product_id, product_name, product_category, product_price, " +
            "product_quantity, product_brand, customer_id, transaction_date, payment_method, total_amount) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (transaction_id) DO UPDATE SET " +
            "product_id = EXCLUDED.product_id, " +
            "product_name  = EXCLUDED.product_name, " +
            "product_category  = EXCLUDED.product_category, " +
            "product_price = EXCLUDED.product_price, " +
            "product_quantity = EXCLUDED.product_quantity, " +
            "product_brand = EXCLUDED.product_brand, " +
            "customer_id  = EXCLUDED.customer_id, " +
            "transaction_date = EXCLUDED.transaction_date, " +
            "payment_method = EXCLUDED.payment_method, " +
            "total_amount  = EXCLUDED.total_amount " +
            "WHERE transactions.transaction_id = EXCLUDED.transaction_id",
        type_info= type_info,
        jdbc_connection_options= connOptions,
        jdbc_execution_options= execOptions
    )

    transaction_stream.add_sink(insert_transactions)
    print("-----Insert Into transactions Table Sink Created-----")

    # Insert Into sales_per_category Table Sink  
    per_category_type_info = Types.ROW_NAMED(
        ["transactionDate","category", "totalSales"],
        [Types.SQL_TIMESTAMP(), Types.STRING(), Types.DOUBLE()]
        )
      
    per_category_stream = transaction_stream.map(lambda x: per_category(x), output_type=per_category_type_info) \
                                            .key_by(lambda x: x['category']) \
                                            .sum('totalSales')

    insert_per_category = JdbcSink.sink(
        "INSERT INTO sales_per_category(transaction_date, category, total_sales) " +
            "VALUES (?, ?, ?) " +
            "ON CONFLICT (transaction_date, category) DO UPDATE SET " +
            "total_sales = EXCLUDED.total_sales " +
            "WHERE sales_per_category.category = EXCLUDED.category " +
            "AND sales_per_category.transaction_date = EXCLUDED.transaction_date",
        type_info= per_category_type_info,
        jdbc_connection_options= connOptions,
        jdbc_execution_options= execOptions
    )
    
    per_category_stream.add_sink(insert_per_category)
    print("-----Insert Into sales_per_category Table Sink Created-----")

    # Insert Into sales_per_day Table Sink  
    per_day_type_info = Types.ROW_NAMED(
        ["transactionDate", "totalSales"],
        [Types.SQL_TIMESTAMP(), Types.DOUBLE()]
        )
      
    per_day_stream = transaction_stream.map(lambda x: per_day(x), output_type=per_day_type_info) \
                                       .key_by(lambda x: x['transactionDate']) \
                                       .sum('totalSales')

    insert_per_day = JdbcSink.sink(
        "INSERT INTO sales_per_day(transaction_date, total_sales) " +
            "VALUES (?,?) " +
            "ON CONFLICT (transaction_date) DO UPDATE SET " +
            "total_sales = EXCLUDED.total_sales " +
            "WHERE sales_per_day.transaction_date = EXCLUDED.transaction_date",
        type_info= per_day_type_info,
        jdbc_connection_options= connOptions,
        jdbc_execution_options= execOptions
    )
    
    per_day_stream.add_sink(insert_per_day)
    print("-----Insert Into sales_per_day Table Sink Created-----")

    # Insert Into sales_per_month Table Sink  
    per_month_type_info = Types.ROW_NAMED(
        ["year", "month", "totalSales"],
        [Types.INT(), Types.INT(), Types.DOUBLE()]
        )
      
    per_month_stream = transaction_stream.map(lambda x: per_month(x), output_type=per_month_type_info) \
                                       .key_by(lambda x: x['month']) \
                                       .sum('totalSales')
    
    insert_per_month = JdbcSink.sink(
        "INSERT INTO sales_per_month(year, month, total_sales) " +
            "VALUES (?,?,?) " +
            "ON CONFLICT (year, month) DO UPDATE SET " +
            "total_sales = EXCLUDED.total_sales " +
            "WHERE sales_per_month.year = EXCLUDED.year " +
            "AND sales_per_month.month = EXCLUDED.month ",
        type_info= per_month_type_info,
        jdbc_connection_options= connOptions,
        jdbc_execution_options= execOptions
    )
    
    per_month_stream.add_sink(insert_per_month)
    print("-----Insert Into sales_per_month Table Sink Created-----")

    #Elasticsearch Sink
    elastic_stream = transaction_stream.map(lambda x: to_map(x), output_type=Types.MAP(Types.STRING(),Types.STRING()))
                                            
    es7_sink = Elasticsearch7SinkBuilder() \
        .set_hosts(['localhost:9200']) \
        .set_emitter(ElasticsearchEmitter.static_index("transactions", "key")) \
        .set_bulk_flush_max_actions(5) \
        .build()

    elastic_stream.sink_to(es7_sink)
    print("-----Elasticsearch Sink Created-----")

    elastic_stream.print()
    env.execute()

if __name__ == '__main__':
    read_from_kafka()