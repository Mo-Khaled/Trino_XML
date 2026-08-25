-- Bulk-seed ACCOUNT with XMLTYPE rows cloned from the existing sample record.
--
-- First run create_account_table.sql in DBeaver using Execute SQL Script (Alt+X).
-- Then run this script using the same command. It contains only SQL statements:
-- no SQL*Plus slash delimiter and no anonymous PL/SQL block.
-- Change rows_to_insert in the constants CTE to control the generated volume.

INSERT INTO account (recid, xmlrecord, currency)
WITH
  constants AS (
    SELECT
      '9000000112345001' AS source_recid,
      9000000120000001 AS first_generated_id,
      10000 AS rows_to_insert
    FROM dual
  ),
  stylesheet AS (
    SELECT XMLTYPE(q'~
      <xsl:stylesheet version="1.0"
        xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
        <xsl:variable name="rowRecid" select="/row/@id"/>
        <xsl:output method="xml" omit-xml-declaration="yes"/>

        <xsl:template match="@*|node()">
          <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
          </xsl:copy>
        </xsl:template>

        <xsl:template match="row/@id">
          <xsl:attribute name="id">
            <xsl:value-of select="$rowRecid"/>
          </xsl:attribute>
        </xsl:template>

        <xsl:template match="c1" priority="1">
          <xsl:copy><xsl:copy-of select="@*"/>
            <xsl:value-of select="concat('90', substring($rowRecid, 11, 6))"/>
          </xsl:copy>
        </xsl:template>

        <xsl:template match="c2 | c85" priority="1">
          <xsl:copy><xsl:copy-of select="@*"/>
            <xsl:value-of select="concat('65', substring($rowRecid, 15, 1), '0')"/>
          </xsl:copy>
        </xsl:template>

        <xsl:template match="c3 | c5" priority="1">
          <xsl:copy><xsl:copy-of select="@*"/>
            <xsl:value-of select="concat('Test account ', $rowRecid)"/>
          </xsl:copy>
        </xsl:template>

        <xsl:template match="c8 | c93 | c95" priority="1">
          <xsl:copy><xsl:copy-of select="@*"/>
            <xsl:choose>
              <xsl:when test="substring($rowRecid, string-length($rowRecid), 1) = '1'">USD</xsl:when>
              <xsl:when test="substring($rowRecid, string-length($rowRecid), 1) = '2'">EUR</xsl:when>
              <xsl:when test="substring($rowRecid, string-length($rowRecid), 1) = '3'">GBP</xsl:when>
              <xsl:otherwise>EGP</xsl:otherwise>
            </xsl:choose>
          </xsl:copy>
        </xsl:template>

        <xsl:template match="c23 | c24 | c25 | c26 | c27 | c29 | c32 | c35 | c38 | c41 | c44 | c77 | c122 | c149" priority="1">
          <xsl:copy><xsl:copy-of select="@*"/>
            <xsl:value-of select="format-number((number(substring($rowRecid, 11, 6)) mod 900000) div 100, '0.00')"/>
          </xsl:copy>
        </xsl:template>

        <xsl:template match="c28 | c31 | c34 | c37 | c40 | c43 | c46 | c47 | c48 | c49 | c50 | c78 | c79 | c121 | c167" priority="1">
          <xsl:copy><xsl:copy-of select="@*"/>
            <xsl:value-of select="concat('2026', format-number((number(substring($rowRecid, 15, 2)) mod 12) + 1, '00'), format-number((number(substring($rowRecid, 13, 2)) mod 28) + 1, '00'))"/>
          </xsl:copy>
        </xsl:template>

        <xsl:template match="c249 | c251" priority="1">
          <xsl:copy><xsl:copy-of select="@*"/>
            <xsl:value-of select="concat('TEST_USER_', substring($rowRecid, 11, 6))"/>
          </xsl:copy>
        </xsl:template>

        <xsl:template match="*[not(*)]">
          <xsl:copy>
            <xsl:copy-of select="@*"/>
            <xsl:value-of select="."/>
          </xsl:copy>
        </xsl:template>
      </xsl:stylesheet>
    ~') AS xslt
    FROM dual
  ),
  source_row AS (
    SELECT a.xmlrecord, c.source_recid
    FROM account a
    CROSS JOIN constants c
    WHERE a.recid = c.source_recid
  ),
  generated_rows AS (
    SELECT
      TO_CHAR(
        c.first_generated_id + LEVEL - 1,
        'FM99999999999999999999999999999999999999'
      ) AS recid
    FROM constants c
    CONNECT BY LEVEL <= c.rows_to_insert
  )
SELECT
  g.recid,
  XMLTYPE(
    REPLACE(
      XMLSERIALIZE(DOCUMENT s.xmlrecord AS CLOB),
      'id="' || s.source_recid || '"',
      'id="' || g.recid || '"'
    )
  ).transform(x.xslt),
  CASE SUBSTR(g.recid, -1)
    WHEN '1' THEN 'USD'
    WHEN '2' THEN 'EUR'
    WHEN '3' THEN 'GBP'
    ELSE 'EGP'
  END
FROM source_row s
CROSS JOIN generated_rows g
CROSS JOIN stylesheet x
WHERE NOT EXISTS (
  SELECT 1
  FROM account existing_account
  WHERE existing_account.recid = g.recid
);

COMMIT;

SELECT COUNT(*) AS account_row_count
FROM account;
