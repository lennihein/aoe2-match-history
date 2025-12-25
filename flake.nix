{
  description = "AoE2 Scout WebUI";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        pythonDeps = ps: with ps; [
          flask
          requests
          beautifulsoup4
          lxml
          gunicorn
        ];
        pythonEnv = pkgs.python3.withPackages pythonDeps;
      in
      {
        packages.default = pkgs.stdenv.mkDerivation {
          pname = "aoe2-scout";
          version = "0.1.0";
          src = ./.;

          installPhase = ''
            mkdir -p $out/lib/aoe2-scout
            cp -r . $out/lib/aoe2-scout/

            mkdir -p $out/bin
            cat <<EOF > $out/bin/aoe2-scout
            #!${pkgs.runtimeShell}
            export PYTHONPATH="\$PYTHONPATH:$out/lib/aoe2-scout"
            exec ${pythonEnv}/bin/gunicorn --workers 3 --bind 0.0.0.0:5000 --chdir $out/lib/aoe2-scout app:app
            EOF
            chmod +x $out/bin/aoe2-scout
          '';
        };

        devShells.default = pkgs.mkShell {
          buildInputs = [ pythonEnv ];
        };
      }) // {
        nixosModules.default = { config, lib, pkgs, ... }:
          let
            cfg = config.services.aoe2-scout;
          in
          {
            options.services.aoe2-scout = {
              enable = lib.mkEnableOption "AoE2 Scout WebUI";
              port = lib.mkOption {
                type = lib.types.port;
                default = 5000;
              };
              dataDir = lib.mkOption {
                type = lib.types.path;
                default = "/var/lib/aoe2-scout";
              };
            };

            config = lib.mkIf cfg.enable {
              systemd.services.aoe2-scout = {
                description = "AoE2 Scout WebUI";
                after = [ "network.target" ];
                wantedBy = [ "multi-user.target" ];
                
                serviceConfig = {
                  ExecStart = "${self.packages.${pkgs.system}.default}/bin/aoe2-scout";
                  WorkingDirectory = cfg.dataDir;
                  StateDirectory = "aoe2-scout";
                  DynamicUser = true;
                  Restart = "always";
                };

                environment = {
                  AOE2_DATA_DIR = "/var/lib/aoe2-scout";
                };
              };
            };
          };
      };
}
