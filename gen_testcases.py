from sqlglot.time import format_time

tsql_time_mapping = {
    "y": "%Y",
    "Y": "%Y",
    "YYYY": "%Y",
    "yyyy": "%Y",
    "YY": "%y",
    "yy": "%y",
    "MMMM": "%B",
    "MMM": "%b",
    "MM": "%m",
    "M": "%-m",
    "dd": "%d",
    "d": "%-d",
    "HH": "%H",
    "H": "%-H",
    "hh": "%I",
    "h": "%-I",
    "mm": "%M",
    "m": "%-M",
    "ss": "%S",
    "s": "%-S",
    "S": "%f",
}

hive_time_mapping = {
    "y": "%Y",
    "Y": "%Y",
    "YYYY": "%Y",
    "yyyy": "%Y",
    "YY": "%y",
    "yy": "%y",
    "MMMM": "%B",
    "MMM": "%b",
    "MM": "%m",
    "M": "%-m",
    "dd": "%d",
    "d": "%-d",
    "HH": "%H",
    "H": "%-H",
    "hh": "%I",
    "h": "%-I",
    "mm": "%M",
    "m": "%-M",
    "ss": "%S",
    "s": "%-S",
    "SSSSSS": "%f",
    "a": "%p",
    "DD": "%j",
    "D": "%-j",
    "E": "%a",
}

convert_format_mapping = {
    0: "%b %d %Y %-I:%M%p",
    1: "%m/%d/%y",
    2: "%y.%m.%d",
    3: "%d/%m/%y",
    4: "%d.%m.%y",
    5: "%d-%m-%y",
    6: "%d %b %y",
    7: "%b %d, %y",
    8: "%H:%M:%S",
    9: "%b %d %Y %-I:%M:%S:%f%p",
    10: "mm-dd-yy",
    11: "yy/mm/dd",
    12: "yymmdd",
    13: "%d %b %Y %H:%M:ss:%f",
    14: "%H:%M:%S:%f",
    20: "%Y-%m-%d %H:%M:%S",
    21: "%Y-%m-%d %H:%M:%S.%f",
    22: "%m/%d/%y %-I:%M:%S %p",
    23: "%Y-%m-%d",
    24: "%H:%M:%S",
    25: "%Y-%m-%d %H:%M:%S.%f",
    100: "%b %d %Y %-I:%M%p",
    101: "%m/%d/%Y",
    102: "%Y.%m.%d",
    103: "%d/%m/%Y",
    104: "%d.%m.%Y",
    105: "%d-%m-%Y",
    106: "%d %b %Y",
    107: "%b %d, %Y",
    108: "%H:%M:%S",
    109: "%b %d %Y %-I:%M:%S:%f%p",
    110: "%m-%d-%Y",
    111: "%Y/%m/%d",
    112: "%Y%m%d",
    113: "%d %b %Y %H:%M:%S:%f",
    114: "%H:%M:%S:%f",
    120: "%Y-%m-%d %H:%M:%S",
    121: "%Y-%m-%d %H:%M:%S.%f",
}

reverse_time_mapping = {v: k for k, v in hive_time_mapping.items()}


for key, val in convert_format_mapping.items():
    pFormat = convert_format_mapping[key]
    out = "\tself.validate_all(\n"
    out = out + f"\t    \"CONVERT(NVARCHAR(255), x, {key})\",\n"
    out = out + "\t    write={\n"
    expectedFormat = format_time(pFormat, reverse_time_mapping, None)
    out = out + f"\t\t\"spark\": \"DATE_FORMAT(x, '{expectedFormat}')\",\n"
    out = out + "\t    },\n"
    out = out + "\t)\n"
    # print(out)
    # print(f"SELECT DATE_FORMAT(TO_DATE(''), '{expectedFormat}')")
    print(f"PRINT CONCAT('UNION ALL SELECT TO_TIMESTAMP(''', CONVERT(NVARCHAR(255), DATEADD(HOUR, 3, GETDATE()), {key}), ''', ''{expectedFormat}'')')")

        # self.validate_all(
        #     "CHARINDEX('sub', 'testsubstring')",
        #     write={
        #         "spark": "LOCATE('sub', 'testsubstring')",
        #     },
        # )

