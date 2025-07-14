create procedure [dbo].[get_task_json_to_delta] @task_id varchar(256) as

begin
    select top 1
        *
    from
        [dbo].[task_json_to_delta]
    where
        [task_id] = @task_id
end

GO

