
create   procedure [dbo].[get_task_create_scd2_schema] @task_id varchar(256) as
begin
    select * from [dbo].[task_create_scd2_schema] where [task_id] = @task_id
end;

GO

