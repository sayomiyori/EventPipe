import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError

from transform_service.app.config import Settings


def create_s3_session() -> aioboto3.Session:
    return aioboto3.Session()


def _s3_client_kwargs(settings: Settings) -> dict:
    return {
        "endpoint_url": settings.minio_endpoint_url,
        "aws_access_key_id": settings.minio_access_key,
        "aws_secret_access_key": settings.minio_secret_key,
        "region_name": settings.minio_region,
        "config": Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    }


async def upload_raw_json(
    s3_session: aioboto3.Session,
    settings: Settings,
    *,
    key: str,
    body: bytes,
) -> None:
    async with s3_session.client("s3", **_s3_client_kwargs(settings)) as client:
        await client.put_object(
            Bucket=settings.minio_bucket_raw,
            Key=key,
            Body=body,
            ContentType="application/json",
        )


async def ensure_bucket_exists(s3_session: aioboto3.Session, settings: Settings) -> None:
    async with s3_session.client("s3", **_s3_client_kwargs(settings)) as client:
        try:
            await client.create_bucket(Bucket=settings.minio_bucket_raw)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                return
            raise
