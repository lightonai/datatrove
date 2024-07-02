from datatrove.executor.slurm import SlurmPipelineExecutor
from datatrove.pipeline.dedup import MinhashDedupSignature
from datatrove.pipeline.dedup.minhash import (
    MinhashConfig,
    MinhashDedupBuckets,
    MinhashDedupCluster,
    MinhashDedupFilter,
)
from datatrove.pipeline.readers import JsonlReader
from datatrove.pipeline.tokens import TokensCounter
from datatrove.pipeline.writers.jsonl import JsonlWriter


# you can also change ngrams or the number of buckets and their size here
minhash_config = MinhashConfig(
    use_64bit_hashes=True,  # better precision -> fewer false positives (collisions)
    num_buckets=14,
    hashes_per_bucket=8,
    n_grams=5,
)


PART = "minhash-dedup"


S3_FILTERED_PATH = f""
S3_LOGS_FOLDER = f"{S3_FILTERED_PATH}/logs/{PART}"
LOCAL_LOGS_FOLDER = f"/logs/{PART}"
S3_DATA_PATH = f""

TOTAL_TASKS = 25

# SLURM
sbatch_args = {"account": ""}
QoS = "default"
PARTITION = "gpu"


INPUT_READER = JsonlReader(
    data_folder=S3_DATA_PATH, text_key="content", id_key="data_id", recursive=False, progress=False
)

# stage 1 computes minhash signatures for each task (each task gets a set of files)
stage1 = SlurmPipelineExecutor(
    job_name="mh1",
    pipeline=[
        INPUT_READER,
        MinhashDedupSignature(output_folder=f"{S3_FILTERED_PATH}/filtered/_{PART}/signatures", config=minhash_config),
    ],
    tasks=TOTAL_TASKS,
    time="8:00:00",
    partition=PARTITION,
    logging_dir=f"{S3_LOGS_FOLDER}/signatures",
    slurm_logs_folder=f"{LOCAL_LOGS_FOLDER}/signatures/slurm_logs",
    qos=QoS,
    sbatch_args=sbatch_args,
    max_array_launch_parallel=True,
)

# stage 2 finds matches between signatures in each bucket
stage2 = SlurmPipelineExecutor(
    job_name="mh2",
    pipeline=[
        MinhashDedupBuckets(
            input_folder=f"{S3_FILTERED_PATH}/filtered/_{PART}/signatures",
            output_folder=f"{S3_FILTERED_PATH}/filtered/_{PART}/buckets",
            config=minhash_config,
        ),
    ],
    tasks=minhash_config.num_buckets * 4,  # the code supports parallelizing each bucket. here we run 10
    # workers per bucket
    randomize_start=True,
    time="04:00:00",
    cpus_per_task=4,  # you can add run more (smaller) tasks if you do not have a lot of memory
    partition=PARTITION,
    logging_dir=f"{S3_LOGS_FOLDER}/buckets",
    depends=stage1,
    slurm_logs_folder=f"{LOCAL_LOGS_FOLDER}/buckets/slurm_logs",
    qos=QoS,
    sbatch_args=sbatch_args,
)

# stage 3 creates clusters of duplicates using the results from all buckets
stage3 = SlurmPipelineExecutor(
    job_name="mh3",
    pipeline=[
        MinhashDedupCluster(
            input_folder=f"{S3_FILTERED_PATH}/filtered/_{PART}/buckets",
            output_folder=f"{S3_FILTERED_PATH}/filtered/_{PART}/remove_ids",
            config=minhash_config,
        ),
    ],
    tasks=1,  # this step runs on a single task
    time="20:00:00",  # and can also be quite slow. Usually not this slow though
    partition=PARTITION,
    logging_dir=f"{S3_LOGS_FOLDER}/clusters",
    # mem_per_cpu_gb=16, #3.8GB or 16GB
    cpus_per_task=8,  # 2
    qos=QoS,
    sbatch_args=sbatch_args,
    depends=stage2,
    slurm_logs_folder=f"{LOCAL_LOGS_FOLDER}/clusters/slurm_logs",
)

# stage 4 reads the original input data and removes all but 1 sample per duplicate cluster
# the data must match exactly stage 1, so number of tasks and the input source must be the same
stage4 = SlurmPipelineExecutor(
    job_name="mh4",
    pipeline=[
        INPUT_READER,
        TokensCounter(
            tokenizer_name_or_path="aubmindlab/aragpt2-base"
        ),  # nice way to see how many tokens we had before and after deduplication
        MinhashDedupFilter(
            input_folder=f"{S3_FILTERED_PATH}/filtered/_{PART}/remove_ids",
            exclusion_writer=JsonlWriter(f"{S3_FILTERED_PATH}/filtered/_{PART}/removed"),
        ),
        JsonlWriter(output_folder=f"{S3_FILTERED_PATH}/deduped/{PART}"),
    ],
    tasks=TOTAL_TASKS,
    time="8:00:00",
    partition=PARTITION,
    logging_dir=f"{S3_LOGS_FOLDER}/filter",
    qos=QoS,
    sbatch_args=sbatch_args,
    depends=stage3,
    slurm_logs_folder=f"{LOCAL_LOGS_FOLDER}/filter/slurm_logs",
)


# launch dedup pipelines
stage4.run()
