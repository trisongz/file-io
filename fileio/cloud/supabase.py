from fileio.cloud import auth
from fileio.configs import CloudConfig
from fileio.static.classes import SupabaseConfig

from supabase_py import create_client


def _auth_supabase(
    supabase_url: str = CloudConfig.SupabaseURL,
    supabase_key: str = CloudConfig.SupabaseKey,
    ):
    auth.SUPABASE_AUTH = SupabaseConfig(supabase_url, supabase_key)
    client = create_client(supabase_url = supabase_url, supabase_key = supabase_key)
    auth.SUPABASE_CLIENT = client.storage()