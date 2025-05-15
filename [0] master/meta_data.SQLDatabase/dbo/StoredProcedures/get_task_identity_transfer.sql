
create   procedure [dbo].[get_task_identity_transfer] @task_id varchar(256) as
begin
    select top 1 * from [dbo].[task_identity_transfer] where [task_id] = @task_id
end;

GO

