create   view control_task_target as

select distinct
    [task_target].[task_id],
    [task_target].[target_path] as [dataset_path]
from
    (select [task_id], [target_path] from [dbo].[task_pokemon_api]

    union all

    select [task_id], [target_path] from [dbo].[task_json_to_delta]

    union all

    select [task_id], [target_path] from [dbo].[task_parquet_to_delta]

    union all
    
    select [task_id], [target_path] from [dbo].[task_update_silver]

    union all

    select [task_id], [target_path] from [dbo].[task_create_scd2]
    
    union all

    select [task_id], [target_path] from [dbo].[task_identity_transfer]
    ) as [task_target]

GO

