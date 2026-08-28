{
  description = "Logma single-container runtime cell";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };

      s6Noarch = pkgs.fetchurl {
        url = "https://github.com/just-containers/s6-overlay/releases/download/v3.2.3.2/s6-overlay-noarch.tar.xz";
        sha256 = "5379750ed30a84bbd2e2dd74847ba6b5bd29cd0b2e3ea2ec58049b57eb2eda12";
      };

      s6Arch = pkgs.fetchurl {
        url = "https://github.com/just-containers/s6-overlay/releases/download/v3.2.3.2/s6-overlay-x86_64.tar.xz";
        sha256 = "e6befcc96a437a3831386ecfc51808c5d3e939dc5fe3c02ae9284599e8aa2408";
      };

      s6Root = pkgs.runCommand "s6-overlay-root" { nativeBuildInputs = [ pkgs.xz pkgs.gnutar ]; } ''
        mkdir -p "$out"
        tar -C "$out" -Jxpf ${s6Noarch}
        tar -C "$out" -Jxpf ${s6Arch}
      '';

      logma = pkgs.buildGoModule {
        pname = "logma";
        version = "0.1.0";
        src = self;
        vendorHash = "sha256-N7SJVbkjtlhr+GDxJs4vsskr39qJlaeuzUKHZ7pcW5I=";
        subPackages = [ "cmd/api" ];
        postInstall = ''
          mv "$out/bin/api" "$out/bin/logma"
        '';
      };

      runtimeRoot = pkgs.runCommand "logma-runtime-root" {} ''
        mkdir -p           "$out/etc/logma"           "$out/etc/nginx"           "$out/etc/s6-overlay/s6-rc.d/user/contents.d"           "$out/etc/s6-overlay/s6-rc.d/init-dirs"           "$out/etc/s6-overlay/s6-rc.d/redis/dependencies.d"           "$out/etc/s6-overlay/s6-rc.d/logma/dependencies.d"           "$out/etc/s6-overlay/s6-rc.d/nginx/dependencies.d"

        cat > "$out/etc/logma/redis.conf" <<'EOF'
        port 0
        protected-mode yes
        save ""
        appendonly no
        unixsocket /run/redis/redis.sock
        unixsocketperm 0770
        dir /run/redis
        EOF

        cat > "$out/etc/nginx/nginx.conf" <<'EOF'
        worker_processes 1;
        error_log /dev/stderr notice;
        pid /run/nginx.pid;

        events { worker_connections 256; }

        http {
          access_log /dev/stdout;
          server {
            listen 8080;
            location / {
              proxy_http_version 1.1;
              proxy_set_header Host $host;
              proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
              proxy_set_header X-Forwarded-Proto $scheme;
              proxy_pass http://127.0.0.1:18080;
            }
          }
        }
        EOF

        echo oneshot > "$out/etc/s6-overlay/s6-rc.d/init-dirs/type"
        cat > "$out/etc/s6-overlay/s6-rc.d/init-dirs/up" <<'EOF'
        #!/command/execlineb -P
        foreground { mkdir -p /run/redis }
        foreground { chmod 0770 /run/redis }
        foreground { mkdir -p /var/lib/nginx }
        foreground { mkdir -p /var/log/nginx }
        EOF
        chmod +x "$out/etc/s6-overlay/s6-rc.d/init-dirs/up"

        echo longrun > "$out/etc/s6-overlay/s6-rc.d/redis/type"
        touch "$out/etc/s6-overlay/s6-rc.d/redis/dependencies.d/init-dirs"
        cat > "$out/etc/s6-overlay/s6-rc.d/redis/run" <<EOF
        #!/command/with-contenv sh
        exec ${pkgs.redis}/bin/redis-server /etc/logma/redis.conf
        EOF
        chmod +x "$out/etc/s6-overlay/s6-rc.d/redis/run"

        echo longrun > "$out/etc/s6-overlay/s6-rc.d/logma/type"
        touch "$out/etc/s6-overlay/s6-rc.d/logma/dependencies.d/redis"
        cat > "$out/etc/s6-overlay/s6-rc.d/logma/run" <<EOF
        #!/command/with-contenv sh
        export REDIS_NETWORK=unix
        export REDIS_SOCKET=/run/redis/redis.sock
        exec ${logma}/bin/logma 18080
        EOF
        chmod +x "$out/etc/s6-overlay/s6-rc.d/logma/run"

        echo longrun > "$out/etc/s6-overlay/s6-rc.d/nginx/type"
        touch "$out/etc/s6-overlay/s6-rc.d/nginx/dependencies.d/logma"
        cat > "$out/etc/s6-overlay/s6-rc.d/nginx/run" <<EOF
        #!/command/with-contenv sh
        exec ${pkgs.nginx}/bin/nginx -c /etc/nginx/nginx.conf -g 'daemon off;'
        EOF
        chmod +x "$out/etc/s6-overlay/s6-rc.d/nginx/run"

        touch "$out/etc/s6-overlay/s6-rc.d/user/contents.d/init-dirs"
        touch "$out/etc/s6-overlay/s6-rc.d/user/contents.d/redis"
        touch "$out/etc/s6-overlay/s6-rc.d/user/contents.d/logma"
        touch "$out/etc/s6-overlay/s6-rc.d/user/contents.d/nginx"
      '';

      image = pkgs.dockerTools.buildLayeredImage {
        name = "logma-cell";
        tag = "latest";
        contents = [
          pkgs.bash
          pkgs.coreutils
          pkgs.redis
          pkgs.nginx
          pkgs.cacert
          logma
          s6Root
          runtimeRoot
        ];
        config = {
          Entrypoint = [ "/init" ];
          ExposedPorts = { "8080/tcp" = {}; };
          Env = [
            "S6_KEEP_ENV=1"
            "REDIS_NETWORK=unix"
            "REDIS_SOCKET=/run/redis/redis.sock"
          ];
        };
        maxLayers = 100;
      };
    in {
      packages.${system} = {
        default = image;
        logma = logma;
        image = image;
      };
    };
}
