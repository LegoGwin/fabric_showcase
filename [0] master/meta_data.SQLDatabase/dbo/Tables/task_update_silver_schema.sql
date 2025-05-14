CREATE TABLE [dbo].[task_update_silver_schema] (
    [task_id]         VARCHAR (256) NULL,
    [expression]      VARCHAR (256) NULL,
    [column_type]     VARCHAR (64)  NULL,
    [column_name]     VARCHAR (64)  NULL,
    [column_order]    INT           NULL,
    [is_filter]       BIT           NULL,
    [is_primary_key]  BIT           NULL,
    [is_batch_key]    BIT           NULL,
    [is_order_by]     INT           NULL,
    [is_output]       BIT           NULL,
    [is_partition_by] INT           NULL
);


GO

