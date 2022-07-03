from sys import version_info

if version_info.major > 2:
    from compliance import cli
else:
    raise NotImplementedError('Protocol Compliance requires Python 3 or higher. '
                              'Upgrade to Python 3+ or use [virtual] environments.')


def main():
    """Entry point."""
    cli.main()


if __name__ == '__main__':
    main()
