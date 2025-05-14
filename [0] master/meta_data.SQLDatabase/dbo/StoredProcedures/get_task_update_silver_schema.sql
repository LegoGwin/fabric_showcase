
create   procedure [dbo].[get_task_update_silver_schema] @task_id varchar(256) as

begin
    select
        *
    from
        [dbo].[task_update_silver_schema]
    where
        [task_id] = @task_id
end;

GO

