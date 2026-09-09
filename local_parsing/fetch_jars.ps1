# Downloads the Spark jars the local runner needs into local_parsing/jars/.
#
# Versions mirror spark-operator/spark-custom-image/Dockerfile so parsing / write
# behaviour matches the bank's production image (Spark 3.5.5 / Scala 2.12).
# ojdbc8 is pulled from Maven Central (the Dockerfile's OTN URL needs a license
# click); it is the same 23.7 line.

$ErrorActionPreference = "Stop"
$jarsDir = Join-Path $PSScriptRoot "jars"
New-Item -ItemType Directory -Force -Path $jarsDir | Out-Null

$mvn = "https://repo1.maven.org/maven2"
$jars = [ordered]@{
    "spark-xml_2.12-0.15.0.jar"                   = "$mvn/com/databricks/spark-xml_2.12/0.15.0/spark-xml_2.12-0.15.0.jar"
    "iceberg-spark-runtime-3.5_2.12-1.6.1.jar"    = "$mvn/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar"
    "iceberg-spark-extensions-3.5_2.12-1.6.1.jar" = "$mvn/org/apache/iceberg/iceberg-spark-extensions-3.5_2.12/1.6.1/iceberg-spark-extensions-3.5_2.12-1.6.1.jar"
    "iceberg-aws-bundle-1.6.1.jar"                = "$mvn/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar"
    "hadoop-aws-3.3.4.jar"                        = "$mvn/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
    "aws-java-sdk-bundle-1.12.262.jar"            = "$mvn/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
    # Oracle JDBC trio - only the `history` job needs these (daily uses python-oracledb).
    "ojdbc8.jar"                                  = "$mvn/com/oracle/database/jdbc/ojdbc8/23.7.0.25.01/ojdbc8-23.7.0.25.01.jar"
    "xmlparserv2-19.3.0.0.jar"                    = "$mvn/com/oracle/database/xml/xmlparserv2/19.3.0.0/xmlparserv2-19.3.0.0.jar"
    "xdb-19.3.0.0.jar"                            = "$mvn/com/oracle/database/xml/xdb/19.3.0.0/xdb-19.3.0.0.jar"
}

foreach ($name in $jars.Keys) {
    $dest = Join-Path $jarsDir $name
    if (Test-Path $dest) {
        $mb = "{0:N1}" -f ((Get-Item $dest).Length / 1MB)
        Write-Host "skip  $name  ($mb MB)"
        continue
    }
    Write-Host "get   $name"
    Invoke-WebRequest -Uri $jars[$name] -OutFile $dest -UseBasicParsing
    $mb = "{0:N1}" -f ((Get-Item $dest).Length / 1MB)
    Write-Host "  ok  $mb MB"
}

Write-Host ""
Write-Host "jars in $jarsDir :"
Get-ChildItem $jarsDir -Filter *.jar | ForEach-Object {
    "{0,-46} {1,8:N1} MB" -f $_.Name, ($_.Length / 1MB)
}
