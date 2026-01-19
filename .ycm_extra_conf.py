def Settings( **kwargs ):
    if kwargs.get("language") == "cfamily":
        return {
            "flags": [
                "-O3",
                "-Wall",
                "-Wextra",
                "-Werror",
                "-Wno-unused-parameter",
                "-std=c11",
                "-march=native",
                "-isystem",
                "/usr/include/python3.13/",
            ],
        }
