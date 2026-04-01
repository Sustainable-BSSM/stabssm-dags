import os

from dotenv import load_dotenv

load_dotenv()


class DiscordConfig:
    BOT_API_KEY: str = os.getenv("DISCORD_BOT_API_KEY")
    GUILD_ID: str = os.getenv("DISCORD_GUILD_ID")
    FORUM_CHANNEL_ID: str = os.getenv("DISCORD_FORUM_CHANNEL_ID")

    @classmethod
    def to_env_dict(cls) -> dict:
        return {
            "DISCORD_BOT_API_KEY": cls.BOT_API_KEY,
            "DISCORD_GUILD_ID": cls.GUILD_ID,
            "DISCORD_FORUM_CHANNEL_ID": cls.FORUM_CHANNEL_ID,
        }
