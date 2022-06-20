CREATE TABLE IF NOT EXISTS `{{ project_id_src  }}.{{ dataset_cdc_processed }}.trends` (
    WeekStart DATE,	
    InterestOverTime	INT64,		
    CountryCode	STRING,		
    HierarchyId	STRING,		
    HierarchyText	STRING		
);

