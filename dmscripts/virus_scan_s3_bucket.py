from collections import Counter
from itertools import chain
import logging


logger = logging.getLogger("script")


def virus_scan_bucket(
    s3_client,
    antivirus_api_client,
    bucket_name,
    prefix="",
    since=None,
    dry_run=True,
    map_callable=map,
):
    def handle_version(version):
        counters_to_increment = set()

        if since and version.get('LastModified') and version['LastModified'] < since:
            logger.debug("Ignoring file from %s: %s", version["LastModified"], version["Key"])
            return ()

        logger.info(
            f"{'(Would be) ' if dry_run else ''}Requesting scan of key %s version %s (%s)",
            version["Key"],
            version["VersionId"],
            version["LastModified"],
        )
        counters_to_increment.add("candidate")

        if not dry_run:
            result = antivirus_api_client.scan_and_tag_s3_object(
                bucket_name,
                version["Key"],
                version["VersionId"],
            )

            if result["avStatusApplied"]:
                if result.get("newAvStatus", {}).get("avStatus.result") == "pass":
                    counters_to_increment.add("pass")
                else:
                    counters_to_increment.add("fail")
                message = f"Marked with result {result.get('newAvStatus', {}).get('avStatus.result')}"
            else:
                counters_to_increment.add("already_tagged")
                message = f"Unchanged: "
                if result.get("existingAvStatus", {}).get("avStatus.result"):
                    message += f"already marked as {result['existingAvStatus']['avStatus.result']!r}"
                    if result.get("existingAvStatus", {}).get("avStatus.ts"):
                        message += f" ({result['existingAvStatus']['avStatus.ts']})"

            logger.info("%s: %s", version["VersionId"], message)

        return counters_to_increment

    counter = Counter()
    try:
        for _counters_to_increment in map_callable(
            handle_version,
            chain.from_iterable(
                page.get("Versions") or ()
                for page in s3_client.get_paginator("list_object_versions").paginate(
                    Bucket=bucket_name,
                    Prefix=prefix,
                )
            ),
        ):
            counter.update(_counters_to_increment)
    except BaseException as e:
        logger.warning("Aborting with counter = %s", counter)
        raise

    return counter