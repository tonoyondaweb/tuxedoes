"""Main entry point for discovery CLI."""

import argparse
import sys
from discovery.config import load_config
from discovery.config.validator import validate_config
from discovery.utils.errors import ConfigValidationError
from discovery.utils.logging import logger


def main():
    """Main entry point for discovery CLI."""
    parser = argparse.ArgumentParser(
        description='Snowflake environment discovery tool',
        prog='discovery'
    )

    subparsers = parser.add_subparsers(dest='command', help='Sub-command')

    # extract subcommand
    extract_parser = subparsers.add_parser('extract', help='Extract metadata from Snowflake')
    extract_parser.add_argument(
        '--config',
        required=True,
        help='Path to discovery config YAML file'
    )

    # diff subcommand
    diff_parser = subparsers.add_parser('diff', help='Compare discovery outputs')
    diff_parser.add_argument(
        '--config',
        required=True,
        help='Path to discovery config YAML file'
    )

    # validate-config subcommand
    validate_parser = subparsers.add_parser('validate-config', help='Validate discovery config YAML')
    validate_parser.add_argument(
        'config_file',
        help='Path to discovery config YAML file'
    )

    args = parser.parse_args()

    try:
        if args.command == 'extract':
            logger.info(f"Extracting metadata from config: {args.config}")
            from discovery.orchestrator import run_extraction, ExtractionResult
            from discovery.utils.errors import PartialExtractionError

            try:
                result = run_extraction(args.config)
                logger.info(
                    f"Extraction completed successfully: "
                    f"{result.extracted} objects extracted, "
                    f"{result.failed} failed, "
                    f"{result.duration:.2f}s"
                )
                sys.exit(0)
            except PartialExtractionError as e:
                logger.warning(f"Extraction completed with partial failures: {e}")
                # Exit with code 1 to indicate some failures, but not complete failure
                sys.exit(1)
        elif args.command == 'diff':
            logger.info(f"Running diff with config: {args.config}")
            config = load_config(args.config)
            validate_config(config)
            # TODO: Implement diff logic in Task 12
            logger.info("Diff command not yet implemented")
            sys.exit(1)
        elif args.command == 'validate-config':
            logger.info(f"Validating config: {args.config_file}")
            config = load_config(args.config_file)
            validate_config(config)
            logger.info("Config is valid")
            sys.exit(0)
        else:
            parser.print_help()
            sys.exit(1)

    except ConfigValidationError as e:
        logger.error(f"Configuration validation failed: {e}")
        sys.exit(1)
    except FileNotFoundError as e:
        logger.error(f"Config file not found: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
