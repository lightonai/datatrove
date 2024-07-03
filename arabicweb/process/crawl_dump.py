import sys
import os


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from datatrove.executor.base import PipelineExecutor
from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.readers import WarcReader
from datatrove.pipeline.writers import JsonlWriter
from datatrove.pipeline.extractors import Trafilatura
from datatrove.pipeline.filters import LanguageFilter
from pipelines.filters import GopherQualityFilter
from datatrove.pipeline.tokens import TokensCounter
from datatrove.utils.typeshelper import Languages


PART = "crawl-dump"
S3_FILTERED_PATH = f""
S3_LOGS_FOLDER = f"{S3_FILTERED_PATH}/logs/{PART}"
S3_DATA_PATH = f""


INPUT_READER = WarcReader(data_folder=S3_DATA_PATH, file_progress=True)

# set the stop words file path to the dir of your custom stop words list


def run_pipeline():
    pipeline = [
        INPUT_READER,
        Trafilatura(favour_precision=True, timeout=0.1),
        LanguageFilter(
            languages=(Languages.english, Languages.arabic),
            exclusion_writer=JsonlWriter(output_folder=f"{S3_FILTERED_PATH}/{PART}/removed-lang"),
        ),
        GopherQualityFilter(
            max_ellipsis_lines_ratio=0.4,
            stop_words_file_path="",
            exclusion_writer=JsonlWriter(output_folder=f"{S3_FILTERED_PATH}/{PART}/removed-gopher"),
        ),
        JsonlWriter(output_folder=f"{S3_FILTERED_PATH}/{PART}/output"),
        TokensCounter(tokenizer_name_or_path="aubmindlab/aragpt2-base"),
    ]

    executor: PipelineExecutor = LocalPipelineExecutor(
        pipeline=pipeline,
        workers=44,
        tasks=50,
        logging_dir=f"{S3_LOGS_FOLDER}/crawl-dump",
    )

    print(executor.run())


if __name__ == "__main__":
    run_pipeline()
