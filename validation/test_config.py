from fileio import File, settings


settings.gcp.update_auth(
    gcp_project = 'my-project',
    google_application_credentials = 'my-credentials.json'
)

print(settings.gcp)

settings.update_auth(
    aws = {
        'aws_access_key_id': 'my-access'
    }
)

print(settings.aws)
