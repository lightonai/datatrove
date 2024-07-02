from datatrove.executor.base import PipelineExecutor
from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.readers import JsonlReader
from datatrove.pipeline.tokens import (
    DocumentTokenizer,
    DocumentTokenizerMerger,
)


PART = "tokenization"
S3_FILTERED_PATH = f""
S3_LOGS_FOLDER = f"{S3_FILTERED_PATH}/logs/{PART}"
S3_DATA_PATH = f""


INPUT_READER = JsonlReader(
    data_folder=S3_DATA_PATH,
    recursive=True,
    file_progress=True,
)


def run_pipeline():
    pipeline_1 = [
        INPUT_READER,
        DocumentTokenizer(
            output_folder=f"{S3_FILTERED_PATH}/{PART}/tokenized-document",
            tokenizer_name_or_path="aubmindlab/aragpt2-base",
            eos_token="<|endoftext|>",
            save_filename="data-tokenized",
        ),
    ]

    pipeline_2 = [
        DocumentTokenizerMerger(
            input_folder=f"{S3_FILTERED_PATH}/{PART}/tokenized-document",
            output_folder=f"{S3_FILTERED_PATH}/{PART}/merged-document",
            save_filename="data-merged",
        ),
    ]

    executor_1: PipelineExecutor = LocalPipelineExecutor(
        pipeline=pipeline_1,
        workers=50,
        tasks=50,
        logging_dir=f"{S3_LOGS_FOLDER}/documents-tokenized",
    )

    # Tasks should be set to 1 as it's one single file
    executor_2: PipelineExecutor = LocalPipelineExecutor(
        pipeline=pipeline_2,
        workers=50,
        tasks=1,
        logging_dir=f"{S3_LOGS_FOLDER}/documents-merged",
    )

    print(executor_1.run())
    print(executor_2.run())


if __name__ == "__main__":
    run_pipeline()
