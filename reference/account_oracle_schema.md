COLUMN_ID|COLUMN_NAME     |DATA_TYPE|FULL_DATA_TYPE|NULLABLE|DATA_DEFAULT                                                                                    |
---------+----------------+---------+--------------+--------+------------------------------------------------------------------------------------------------+
        1|RECID           |VARCHAR2 |VARCHAR2(255) |N       |                                                                                                |
        2|XMLRECORD       |XMLTYPE  |XMLTYPE       |Y       |                                                                                                |
        3|CURRENCY        |VARCHAR2 |VARCHAR2(250) |Y       |CAST(EXTRACTVALUE(SYS_MAKEXML("SYS_NC00003$"),'/row/c8[position()=1]') AS VARCHAR2(250 Byte))   |
        4|CO_CODE         |VARCHAR2 |VARCHAR2(250) |Y       |CAST(EXTRACTVALUE(SYS_MAKEXML("SYS_NC00003$"),'/row/c252[position()=1]') AS VARCHAR2(250 Byte)) |
        5|CATEGORY        |NUMBER   |NUMBER(38,0)  |Y       |CAST(TO_NUMBER(EXTRACTVALUE(SYS_MAKEXML("SYS_NC00003$"),'/row/c2[position()=1]')) AS NUMBER(38))|
        6|CUSTOMER        |NUMBER   |NUMBER(38,0)  |Y       |CAST(TO_NUMBER(EXTRACTVALUE(SYS_MAKEXML("SYS_NC00003$"),'/row/c1[position()=1]')) AS NUMBER(38))|
        7|MNEMONIC        |VARCHAR2 |VARCHAR2(250) |Y       |CAST(EXTRACTVALUE(SYS_MAKEXML("SYS_NC00003$"),'/row/c6[position()=1]') AS VARCHAR2(250 Byte))   |
        8|CURR_NO         |VARCHAR2 |VARCHAR2(250) |Y       |CAST(EXTRACTVALUE(SYS_MAKEXML("SYS_NC00003$"),'/row/c248[position()=1]') AS VARCHAR2(250 Byte)) |
        9|POSTING_RESTRICT|VARCHAR2 |VARCHAR2(250) |Y       |CAST(EXTRACTVALUE(SYS_MAKEXML("SYS_NC00003$"),'/row/c13[position()=1]') AS VARCHAR2(250 Byte))  |
       10|OPENING_DATE    |VARCHAR2 |VARCHAR2(250) |Y       |CAST(EXTRACTVALUE(SYS_MAKEXML("SYS_NC00003$"),'/row/c78[position()=1]') AS VARCHAR2(250 Byte))  |
       11|INT_NO_BOOKING  |VARCHAR2 |VARCHAR2(250) |Y       |CAST(EXTRACTVALUE(SYS_MAKEXML("SYS_NC00003$"),'/row/c17[position()=1]') AS VARCHAR2(250 Byte))  |
