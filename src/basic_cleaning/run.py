#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    logger.info('Initiating W&B run')
    run = wandb.init(project = 'nyc_airbnb',
        group = 'basic_cleaning',
        job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact

    # data load
    logger.info('Loading input artifact')
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)

    # data clean
    logger.info('Parform besic cleaning')
    idx = df['price'].between(float(args.min_price), float(args.max_price))
    df = df[idx].copy()
    df['last_review'] = pd.to_datetime(df['last_review'])

    # save artifact
    logger.info('Log output artifact')
    df.to_csv('clean_sample.csv', index=False)
    artifact = wandb.Artifact(
        args.output_artifact,
        type = args.output_type,
        description = args.output_description
    )
    artifact.add_file('clean_sample.csv')
    run.log_artifact(artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact",
        type=str,
        help="name of the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="name of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="type of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="output description",
        required=True
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="minimum price allowed",
        required=True
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="maximum price allowed",
        required=True
    )


    args = parser.parse_args()

    go(args)
