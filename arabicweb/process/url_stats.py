from datatrove.pipeline.stats import URLStats
from datatrove.executor.base import PipelineExecutor
from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.readers import JsonlReader


# This pipline allows us to extract the top urls in the data

PART = "url-stats"


S3_FILTERED_PATH = f""
S3_LOGS_FOLDER = f"{S3_FILTERED_PATH}/logs/{PART}"
S3_DATA_PATH = f""

output_folder = f"{S3_FILTERED_PATH}/{PART}/output"


INPUT_READER = JsonlReader(
    data_folder=S3_DATA_PATH,
    file_progress=True,
)


def run_example():
    pipeline_1 = [
        INPUT_READER,
        URLStats(
            output_folder=output_folder,
            url_field="source",
            topk=150,
            min_doc_count_to_save=1000,
        ),
    ]

    executor_1: PipelineExecutor = LocalPipelineExecutor(
        pipeline=pipeline_1,
        workers=44,
        tasks=1,
        logging_dir=f"{S3_LOGS_FOLDER}/url-stats",
    )

    print(executor_1.run())


if __name__ == "__main__":
    run_example()
