import os

def running_in_docker() -> bool:
    if os.path.exists("/.dockerenv"):
        return True
    return os.getenv("RUNNING_IN_DOCKER") == "1"


def get_mongo_url(config) -> str:
     return (
		config.get("mongo", "url_docker")
		if running_in_docker()
		else config.get("mongo", "url")
	)
