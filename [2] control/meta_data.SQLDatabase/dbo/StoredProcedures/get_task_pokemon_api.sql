create procedure [dbo].[get_task_pokemon_api] @task_id varchar(256) as

begin
    select top 1
        *
    from
        [dbo].[task_pokemon_api]
    where
        [task_id] = @task_id
end

GO

