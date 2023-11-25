#!/usr/bin/env bash

TRINO_SESSION='--session=hive.hive_storage_format=PARQUET --session=delta.compression_codec=GZIP --session=iceberg.compression_codec=GZIP'

SQL_INSERT='INSERT INTO lineitem SELECT * FROM tpch.tiny.lineitem;'
SQL_DELETE='DELETE FROM lineitem WHERE l_orderkey = 1;'
SQL_UPDATE='UPDATE lineitem SET l_quantity = 0.0 WHERE l_orderkey = 3;'
SQL_MERGE='MERGE INTO lineitem AS target USING tpch.tiny.lineitem AS source ON target.l_orderkey = source.l_orderkey AND target.l_partkey = source.l_partkey AND target.l_suppkey = source.l_suppkey WHEN MATCHED AND target.l_orderkey = 1 THEN DELETE WHEN MATCHED AND target.l_orderkey = 3 THEN UPDATE SET l_quantity = 0.0;'
SQL_DROP_TABLE='DROP TABLE IF EXISTS lineitem;'

for FORMAT in iceberg/mor delta/withoutDeletionVectors; do

    if [[ "$FORMAT" == 'iceberg/mor' ]]; then
        OUTPUT_DIR="${FORMAT}/trino"
        FORMAT=iceberg
        TABLE_PROPS=''
    elif [[ "$FORMAT" == 'delta/withoutDeletionVectors' ]]; then
        OUTPUT_DIR="${FORMAT}/trino"
        FORMAT=delta
        TABLE_PROPS=''
    else
        OUTPUT_DIR="${FORMAT}/trino"
        TABLE_PROPS=''
    fi

    rm -rf "$OUTPUT_DIR"
    mkdir -p "$OUTPUT_DIR"

    SQL_CREATE_TABLE="CREATE TABLE lineitem $TABLE_PROPS AS SELECT * FROM tpch.tiny.lineitem;"

    # Setup
    $TRINO_HOME/bin/trino --catalog="$FORMAT" --schema="tpch_$FORMAT" $TRINO_SESSION --execute="$SQL_DROP_TABLE"

    # CREATE
    $TRINO_HOME/bin/trino --catalog="$FORMAT" --schema="tpch_$FORMAT" $TRINO_SESSION --execute="$SQL_CREATE_TABLE"
    mcli cp -r "minio/tpch/$FORMAT/lineitem/" "$OUTPUT_DIR/create"
    $TRINO_HOME/bin/trino --catalog="$FORMAT" --schema="tpch_$FORMAT" $TRINO_SESSION --execute="$SQL_DROP_TABLE"

    # INSERT
    $TRINO_HOME/bin/trino --catalog="$FORMAT" --schema="tpch_$FORMAT" $TRINO_SESSION --execute="$SQL_CREATE_TABLE"
    $TRINO_HOME/bin/trino --catalog="$FORMAT" --schema="tpch_$FORMAT" $TRINO_SESSION --execute="$SQL_INSERT"
    mcli cp -r "minio/tpch/$FORMAT/lineitem/" "$OUTPUT_DIR/insert"
    $TRINO_HOME/bin/trino --catalog="$FORMAT" --schema="tpch_$FORMAT" $TRINO_SESSION --execute="$SQL_DROP_TABLE"

    # DELETE
    $TRINO_HOME/bin/trino --catalog="$FORMAT" --schema="tpch_$FORMAT" $TRINO_SESSION --execute="$SQL_CREATE_TABLE"
    $TRINO_HOME/bin/trino --catalog="$FORMAT" --schema="tpch_$FORMAT" $TRINO_SESSION --execute="$SQL_DELETE"
    mcli cp -r "minio/tpch/$FORMAT/lineitem/" "$OUTPUT_DIR/delete"
    $TRINO_HOME/bin/trino --catalog="$FORMAT" --schema="tpch_$FORMAT" $TRINO_SESSION --execute="$SQL_DROP_TABLE"

    # UPDATE
    $TRINO_HOME/bin/trino --catalog="$FORMAT" --schema="tpch_$FORMAT" $TRINO_SESSION --execute="$SQL_CREATE_TABLE"
    $TRINO_HOME/bin/trino --catalog="$FORMAT" --schema="tpch_$FORMAT" $TRINO_SESSION --execute="$SQL_UPDATE"
    mcli cp -r "minio/tpch/$FORMAT/lineitem/" "$OUTPUT_DIR/update"
    $TRINO_HOME/bin/trino --catalog="$FORMAT" --schema="tpch_$FORMAT" $TRINO_SESSION --execute="$SQL_DROP_TABLE"

    # MERGE
    #$TRINO_HOME/bin/trino --catalog="$FORMAT" --schema="tpch_$FORMAT" $TRINO_SESSION --execute="$SQL_CREATE_TABLE"
    #$TRINO_HOME/bin/trino --catalog="$FORMAT" --schema="tpch_$FORMAT" $TRINO_SESSION --execute="$SQL_MERGE"
    #mcli cp -r "minio/tpch/$FORMAT/lineitem/" "$OUTPUT_DIR/merge"
    #$TRINO_HOME/bin/trino --catalog="$FORMAT" --schema="tpch_$FORMAT" $TRINO_SESSION --execute="$SQL_DROP_TABLE"

done

# Create Hive table for Hudi in Spark
$TRINO_HOME/bin/trino --catalog=hive --schema=tpch_hive $TRINO_SESSION --execute="$SQL_DROP_TABLE"
$TRINO_HOME/bin/trino --catalog=hive --schema=tpch_hive $TRINO_SESSION --execute="$SQL_CREATE_TABLE"
