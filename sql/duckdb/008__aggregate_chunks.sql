copy (select
    a.pilot_assignment_id,
    list(
        struct_pack(
            chunk_id := c.chunk_id,
            chunk_start := epoch(c.chunk_start),
            chunk_end := epoch(c.chunk_end),
            avg_speed_mps := round(c.avg_speed_mps, 2)
        )
    )
from './data/20250101_20251130/assignments_and_logs.parquet' a
join
    './data/20250101_20251130/chunks.parquet' c
    on a.pilot_assignment_id = c.pilot_assignment_id
    and c.chunk_start >= a.valid_start
    and c.chunk_end <= a.valid_end
group by a.pilot_assignment_id
) to './data/20250101_20251130/aggregate_chunks.parquet';
