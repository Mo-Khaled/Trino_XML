#   bash local_parsing/fetch_jars.sh
#aws-java-sdk-bundle-1.12.262.jar                  267.6 MB
#hadoop-aws-3.3.4.jar                                0.9 MB
#iceberg-aws-bundle-1.6.1.jar                       29.9 MB
#iceberg-spark-extensions-3.5_2.12-1.6.1.jar         0.5 MB
#iceberg-spark-runtime-3.5_2.12-1.6.1.jar           39.9 MB
#ojdbc8.jar                                          6.9 MB
#spark-xml_2.12-0.15.0.jar                           0.2 MB
#xdb-19.3.0.0.jar                                    0.3 MB
#xmlparserv2-19.3.0.0.jar                            1.8 MB

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JARS_DIR="$HERE/jars"
mkdir -p "$JARS_DIR"

MVN="https://repo1.maven.org/maven2"

# name=url pairs, in download order (bash 3 compatible - no associative arrays)
JARS=(
  "spark-xml_2.12-0.15.0.jar=$MVN/com/databricks/spark-xml_2.12/0.15.0/spark-xml_2.12-0.15.0.jar"
  "iceberg-spark-runtime-3.5_2.12-1.6.1.jar=$MVN/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar"
  "iceberg-spark-extensions-3.5_2.12-1.6.1.jar=$MVN/org/apache/iceberg/iceberg-spark-extensions-3.5_2.12/1.6.1/iceberg-spark-extensions-3.5_2.12-1.6.1.jar"
  "iceberg-aws-bundle-1.6.1.jar=$MVN/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar"
  "hadoop-aws-3.3.4.jar=$MVN/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
  "aws-java-sdk-bundle-1.12.262.jar=$MVN/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
  # Oracle JDBC trio - both `daily` and `history` need these now (both use the jdbc reader).
  "ojdbc8.jar=$MVN/com/oracle/database/jdbc/ojdbc8/23.7.0.25.01/ojdbc8-23.7.0.25.01.jar"
  "xmlparserv2-19.3.0.0.jar=$MVN/com/oracle/database/xml/xmlparserv2/19.3.0.0/xmlparserv2-19.3.0.0.jar"
  "xdb-19.3.0.0.jar=$MVN/com/oracle/database/xml/xdb/19.3.0.0/xdb-19.3.0.0.jar"
)

for entry in "${JARS[@]}"; do
  name="${entry%%=*}"
  url="${entry#*=}"
  dest="$JARS_DIR/$name"
  if [[ -f "$dest" ]]; then
    mb=$(awk -v b="$(stat -c%s "$dest")" 'BEGIN { printf "%.1f", b/1048576 }')
    echo "skip  $name  (${mb} MB)"
    continue
  fi
  echo "get   $name"
  curl -fsSL -o "$dest" "$url"
  mb=$(awk -v b="$(stat -c%s "$dest")" 'BEGIN { printf "%.1f", b/1048576 }')
  echo "  ok  ${mb} MB"
done

echo ""
echo "jars in $JARS_DIR :"
for f in "$JARS_DIR"/*.jar; do
  [[ -e "$f" ]] || continue
  mb=$(awk -v b="$(stat -c%s "$f")" 'BEGIN { printf "%.1f", b/1048576 }')
  printf "%-46s %8s MB\n" "$(basename "$f")" "$mb"
done
