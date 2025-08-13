CREATE TABLE [dbo].[task_create_scd2_schema] (
    [task_id]          VARCHAR (256) NULL,
    [column_name]      VARCHAR (256) NULL,
    [is_primary_key]   BIT           NULL,
    [is_business_key]  BIT           NULL,
    [is_date_key]      BIT           NULL,
    [is_valid_from]    BIT           NULL,
    [is_valid_to]      BIT           NULL,
    [is_is_current]    BIT           NULL,
    [is_row_hash]      BIT           NULL,
    [is_surrogate_key] BIT           NULL
);


GO

