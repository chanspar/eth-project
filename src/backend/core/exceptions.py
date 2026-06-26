class DatabaseFetchError(Exception):
    """
    데이터베이스 쿼리 도중 발생하는 에러를 캡슐화하는 커스텀 예외입니다.
    비즈니스 로직(Service 계층)에서 HTTP 상태 코드를 몰라도 되게 하기 위해 사용됩니다.
    """
    pass
