from sys import version_info

if version_info.major > 2:
    from mrQA.cli import cli
else:
    raise NotImplementedError('Protocol Compliance requires Python 3 or higher.'
                              'Upgrade to Python 3+ or use environments.')


def main():
    """Entry point."""
    cli()


if __name__ == '__main__':
    main()
