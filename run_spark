#!/usr/bin/env bash
#
# Spark does not reliably delete the bucket after dropping the table, so more
# force is used.
#

SPARK_CONF='--conf spark.sql.parquet.compression.codec=gzip'

SQL_INSERT='INSERT INTO lineitem SELECT /* REPARTITION(1) */ * FROM tpch_hive.lineitem;'
SQL_DELETE='DELETE FROM lineitem WHERE l_orderkey = 1;'
SQL_UPDATE='UPDATE lineitem SET l_quantity = 0.0 WHERE l_orderkey = 3;'
SQL_MERGE='MERGE INTO lineitem AS target USING tpch_hive.lineitem AS source ON target.l_orderkey = source.l_orderkey AND target.l_partkey = source.l_partkey AND target.l_suppkey = source.l_suppkey WHEN MATCHED AND target.l_orderkey = 1 THEN DELETE WHEN MATCHED AND target.l_orderkey = 3 THEN UPDATE SET l_quantity = 0.0;'
SQL_DROP_TABLE='DROP TABLE IF EXISTS lineitem;'

for VARIANT in hudi/cow hudi/mor iceberg/cow iceberg/mor delta/withoutDeletionVectors delta/withDeletionVectors; do

    if [[ "$VARIANT" == 'hudi/cow' ]]; then
        OUTPUT_DIR="${VARIANT}/spark"
        FORMAT=hudi
        TABLE_PROPS="TBLPROPERTIES (type = 'cow')"
    elif [[ "$VARIANT" == 'hudi/mor' ]]; then
        OUTPUT_DIR="${VARIANT}/spark"
        FORMAT=hudi
        TABLE_PROPS="TBLPROPERTIES (type = 'mor')"
    elif [[ "$VARIANT" == 'iceberg/cow' ]]; then
        OUTPUT_DIR="${VARIANT}/spark"
        FORMAT=iceberg
        TABLE_PROPS="TBLPROPERTIES (write.delete.mode = 'copy-on-write', write.update.mode = 'copy-on-write', write.merge.mode = 'copy-on-write', compression = 'gzip')"
    elif [[ "$VARIANT" == 'iceberg/mor' ]]; then
        OUTPUT_DIR="${VARIANT}/spark"
        FORMAT=iceberg
        TABLE_PROPS="TBLPROPERTIES (write.delete.mode = 'merge-on-read', write.update.mode = 'merge-on-read', write.merge.mode = 'merge-on-read', compression = 'gzip')"
    elif [[ "$VARIANT" == 'delta/withoutDeletionVectors' ]]; then
        OUTPUT_DIR="${VARIANT}/spark"
        FORMAT=delta
        TABLE_PROPS=""
    elif [[ "$VARIANT" == 'delta/withDeletionVectors' ]]; then
        OUTPUT_DIR="${VARIANT}/spark"
        FORMAT=delta
        TABLE_PROPS="TBLPROPERTIES (delta.enableDeletionVectors = true)"
    else
        OUTPUT_DIR="${VARIANT}/spark"
        FORMAT="${VARIANT}"
        TABLE_PROPS=''
    fi

    rm -rf "$OUTPUT_DIR"
    mkdir -p "$OUTPUT_DIR"

    SQL_CREATE_SCHEMA="CREATE SCHEMA IF NOT EXISTS tpch_$FORMAT LOCATION 's3a://tpch/$FORMAT';"
    SQL_CREATE_TABLE="CREATE TABLE lineitem USING $FORMAT $TABLE_PROPS AS SELECT /* REPARTITION(1) */ * FROM tpch_hive.lineitem;"

    # Setup
    spark-sql-hms-$FORMAT $SPARK_CONF -e "$SQL_CREATE_SCHEMA"
    spark-sql-hms-$FORMAT --database "tpch_$FORMAT" $SPARK_CONF -e "$SQL_DROP_TABLE"
    mcli rm -r --force "minio/tpch/$FORMAT/lineitem/"

    # CREATE
    spark-sql-hms-$FORMAT --database "tpch_$FORMAT" $SPARK_CONF -e "$SQL_CREATE_TABLE"
    mcli cp -r "minio/tpch/$FORMAT/lineitem/" "$OUTPUT_DIR/create"
    spark-sql-hms-$FORMAT --database "tpch_$FORMAT" $SPARK_CONF -e "$SQL_DROP_TABLE"
    mcli rm -r --force "minio/tpch/$FORMAT/lineitem/"

    # INSERT
    spark-sql-hms-$FORMAT --database "tpch_$FORMAT" $SPARK_CONF -e "$SQL_CREATE_TABLE"
    spark-sql-hms-$FORMAT --database "tpch_$FORMAT" $SPARK_CONF -e "$SQL_INSERT"
    mcli cp -r "minio/tpch/$FORMAT/lineitem/" "$OUTPUT_DIR/insert"
    spark-sql-hms-$FORMAT --database "tpch_$FORMAT" $SPARK_CONF -e "$SQL_DROP_TABLE"
    mcli rm -r --force "minio/tpch/$FORMAT/lineitem/"

    # DELETE
    spark-sql-hms-$FORMAT --database "tpch_$FORMAT" $SPARK_CONF -e "$SQL_CREATE_TABLE"
    spark-sql-hms-$FORMAT --database "tpch_$FORMAT" $SPARK_CONF -e "$SQL_DELETE"
    mcli cp -r "minio/tpch/$FORMAT/lineitem/" "$OUTPUT_DIR/delete"
    spark-sql-hms-$FORMAT --database "tpch_$FORMAT" $SPARK_CONF -e "$SQL_DROP_TABLE"
    mcli rm -r --force "minio/tpch/$FORMAT/lineitem/"

    # UPDATE
    spark-sql-hms-$FORMAT --database "tpch_$FORMAT" $SPARK_CONF -e "$SQL_CREATE_TABLE"
    spark-sql-hms-$FORMAT --database "tpch_$FORMAT" $SPARK_CONF -e "$SQL_UPDATE"
    mcli cp -r "minio/tpch/$FORMAT/lineitem/" "$OUTPUT_DIR/update"
    spark-sql-hms-$FORMAT --database "tpch_$FORMAT" $SPARK_CONF -e "$SQL_DROP_TABLE"
    mcli rm -r --force "minio/tpch/$FORMAT/lineitem/"

    # MERGE
    #spark-sql-hms-$FORMAT --database "tpch_$FORMAT" $SPARK_CONF -e "$SQL_CREATE_TABLE"
    #spark-sql-hms-$FORMAT --database "tpch_$FORMAT" $SPARK_CONF -e "$SQL_MERGE"
    #mcli cp -r "minio/tpch/$FORMAT/lineitem/" "$OUTPUT_DIR/merge"
    #spark-sql-hms-$FORMAT --database "tpch_$FORMAT" $SPARK_CONF -e "$SQL_DROP_TABLE"
    #mcli rm -r --force "minio/tpch/$FORMAT/lineitem/"

done
