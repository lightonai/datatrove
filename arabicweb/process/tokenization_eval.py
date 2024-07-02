from datatrove.executor.base import PipelineExecutor
from datatrove.pipeline.readers import HuggingFaceDatasetReader
from datatrove.pipeline.tokens.merger import DocumentTokenizerMerger
from datatrove.pipeline.tokens.tokenizer import DocumentTokenizer
from datatrove.executor.local import LocalPipelineExecutor


PART = "tokenization-HF-Datset"
S3_FILTERED_PATH = f""
S3_LOGS_FOLDER = f"{S3_FILTERED_PATH}/logs/{PART}"
S3_DATA_PATH = f""


def run_pipeline():
    pipeline_1 = [
        HuggingFaceDatasetReader(
            #   dataset="khalidalt/ultimate_arabic_news"
            dataset="ClusterlabAi/101_billion_arabic_words_dataset",  # dataset name
            dataset_options={
                # "name":"UltimateArabicPrePros",
                "split": "train",
                # any other options that should be passed to load_dataset
            },
            text_key="text",  # the column that actually contains the text to be tokenized
        ),
        DocumentTokenizer(
            output_folder=f"{S3_FILTERED_PATH}/{PART}/tokenized-document",
            tokenizer_name_or_path="aubmindlab/aragpt2-base",
            eos_token="<|endoftext|>",
            save_filename="Arabic-data-eval-tokenized",
        ),
    ]

    pipeline_2 = [
        DocumentTokenizerMerger(
            input_folder=f"{S3_FILTERED_PATH}/{PART}/tokenized-document",
            output_folder=f"{S3_FILTERED_PATH}/{PART}/merged-document",
            save_filename="Arabic-tokenized-data-merged",
        ),
    ]

    executor_1: PipelineExecutor = LocalPipelineExecutor(
        pipeline=pipeline_1,
        workers=50,
        tasks=50,
        logging_dir=f"{S3_LOGS_FOLDER}/documents-tokenization-eval",
    )

    executor_2: PipelineExecutor = LocalPipelineExecutor(
        pipeline=pipeline_2,
        workers=50,
        tasks=1,
        logging_dir=f"{S3_LOGS_FOLDER}/documents-merged-eval",
    )

    print(executor_1.run())
    print(executor_2.run())


if __name__ == "__main__":
    run_pipeline()
