CREATE TABLE IF NOT EXISTS {{ project_id_src  }}.{{ dataset_cdc_processed }}.holiday_calendar (
    HolidayDate	STRING,		
    Description	STRING,		
    CountryCode	STRING,		
    Year	STRING,		
    WeekDay	STRING,		
    QuarterOfYear	INTEGER,		
    Week	INTEGER		
);