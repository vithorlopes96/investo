import os


# TokenFetcher Interface (for DIP and ISP)
class TokenFetcher:
    def fetch_token(self):
        raise NotImplementedError("Subclasses should implement this method")


# EnvTokenFetcher class (fetches tokens from environment variables, SRP)
class EnvTokenFetcher(TokenFetcher):
    def fetch_token(self):
        token = os.getenv('TOKEN')
        if token:
            return token
        else:
            raise ValueError("API token not found in environment variables")
