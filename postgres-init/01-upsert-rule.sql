-- rendered_task_instance_fields INSERT → UPSERT 변환
-- Airflow 3.x Task SDK에서 재시도 시 unique constraint 위반 방지
CREATE
OR REPLACE RULE rendered_task_instance_fields_upsert AS
ON INSERT TO rendered_task_instance_fields
WHERE EXISTS (
    SELECT 1 FROM rendered_task_instance_fields
    WHERE dag_id = NEW.dag_id
      AND task_id = NEW.task_id
      AND run_id = NEW.run_id
      AND map_index = NEW.map_index
)
DO INSTEAD
UPDATE rendered_task_instance_fields
SET rendered_fields = NEW.rendered_fields,
    k8s_pod_yaml    = NEW.k8s_pod_yaml
WHERE dag_id = NEW.dag_id
  AND task_id = NEW.task_id
  AND run_id = NEW.run_id
  AND map_index = NEW.map_index;
