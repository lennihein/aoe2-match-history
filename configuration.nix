{ config, pkgs, ... }:
let
  aoe2-scout = builtins.getFlake "github:lennihein/aoe2-match-history";
in

{
    imports =
        [
            /etc/nixos/configuration.nix
            /etc/nixos/hardware-configuration.nix
            aoe2-scout.nixosModules.default
        ];

    nixpkgs.config = {
        # allow unfree packages
        allowUnfree = true;
        # add old nixpkgs
        packageOverrides = pkgs: {
            # you can either ref a nix-channel:
            # old = import <nixpkgs-old> {
            # or a tarball:
            # lennihein-22-11 = import (fetchTarball "https://github.com/lennihein/nixpkgs/archive/refs/heads/nixos-22.11.zip") {
            #     config = config.nixpkgs.config;
            # };
            # lennihein = import (fetchTarball "https://github.com/lennihein/nixpkgs/archive/refs/heads/master.zip") {
            #    config = config.nixpkgs.config;
            # };
            # unstable = import <nixos-unstable> {
            #     config = config.nixpkgs.config;
            # };
        };
    };

    # enable flakes
    nix.settings.experimental-features = [ "nix-command" "flakes" ];

    # enable networking
    networking.networkmanager.enable = true;

    # Set your time zone.
    time.timeZone = "Europe/Berlin";

    # Select internationalisation properties.
    i18n.defaultLocale = "en_US.UTF-8";

    i18n.extraLocaleSettings = {
        LC_ADDRESS = "de_DE.UTF-8";
        LC_IDENTIFICATION = "de_DE.UTF-8";
        LC_MEASUREMENT = "de_DE.UTF-8";
        LC_MONETARY = "de_DE.UTF-8";
        LC_NAME = "de_DE.UTF-8";
        LC_NUMERIC = "de_DE.UTF-8";
        LC_PAPER = "de_DE.UTF-8";
        LC_TELEPHONE = "de_DE.UTF-8";
        LC_TIME = "de_DE.UTF-8";
    };

    # virtualisation
    virtualisation.podman.enable = true;
    # virtualisation.libvirtd.enable = true;

    # enable doas
    security.doas.enable = true;
    security.doas.extraRules = [{
        users = [ "lenni" ];
        keepEnv = true;
        noPass = true;
    }];

    # enable packages
    programs.fish.enable = true;
    programs.vim = {
        enable = true;
        defaultEditor = true;
    };
    programs.git.enable = true;
    programs.dconf.enable = true;

    # register fish as a shell
    environment.shells = with pkgs; [fish];

    # define user
    users.users.lenni = {
        shell = pkgs.fish;
        openssh.authorizedKeys.keys = [''ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCzQEYS0fsylcZJRr9L3Bu1UP99hHWsw3euV7CIfzFRAyjrLORf904MqM8S6fnu7I+GtyDezOXR8W+3XktXVik6pnvmpHWpUWPpt7/sGG8uM3j49B0qXXz5oEe28YBFWYvYAcRCXsoXErNmVg0ZsuVJphDSKsOWYbdCtDTX95fJUUwNaJbj2dxZe4FShdc0zTJjyOdSW5LMWbYPrIXqxYd+6+WyL4XPNLgtVbivldQ0ASgtpb1JsYsu6AQ4HVeBsX69n4GwkdXDk8WohlGh7pYPQwRnBWY9PURvmkcZ0BgxWaSjVP0SXEq8KLYgii5TVMh0N93iHYn7Ifilm8Z4z/VpA+FX9c/zYaGyrpXOqE0BYb92cDYFN8uNQMVRokQ/luDd6rJjgbPPyvm5jo8TodHZnAnzNpEY8YqTAJuGou+st7ftiVG2NSzRO/jSfPZ84behffVvVQSy2CGPzWnfRpDQog6pVEKATquM8BBQ74TQUtmUbHfhwqkrzPsapo2LmWU= lenni@PC'' ''ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDUwLO6G/7x5rt8BuJuyNf9MXmqvutHMyptUESOrmLjSFsrZmgeL78PV9nVwd3G252DLsM8bciQKUeQVTJqlwAC5CBvHuNN1ZZwTO/iQYbmwXxf8snzogU++8tFMkRwGOuO8CXymUFGEkD9xdYd7B53ZXDYcfaddajWI/SjvdK7tPHoBRcrIBoLMgvfeOdLUM2fyoxgZnGZbWw7DuUBq2tEw3o4GZkfJSIjZy37Nkxtrmo1qSOiig+kKbtDX2aK3jkBmSnzv7SA8n5J1xq0iy0ploDv7UJwBB+DX/OTQNNgIAXiK3oRb447+QhCR0hhpNZ43sRpKl+dlphlXWtjzfSFLXyzaM4ns14KqNuBkOcO6SJokS1Les2sJ2W+WEdQzRkPCCdAp7/mS1mHECTGymozRgDmKi7QtYSCjOBJ+wO3ZfoR/SUbxUwZopK3JQi0T0B49h99LB+gJf4uULrzA361WtOj7rYBzGSUYWGE5tdQd4tSu2jx6KStXEC9mffO/yE= TERMIUS'' ];
        isNormalUser = true;
        description = "Lenni Hein";
        extraGroups = [ "networkmanager" "wheel" "wireshark" "libvirtd" ];
        packages = with pkgs; [
            # requires config
            helix starship kitty
        ];
    };

    environment.systemPackages = with pkgs; [
        # essentials
        wget
        lsof dig file
        atool unzip
        python3 gnumake cmake clang gcc

        # command line tools
        htop gdu neofetch ranger tldr gitui bat fzf ripgrep pwndbg rm-improved eza nvd direnv procs fd duf bottom

        # to maintain certs for adguard
        certbot
    ];

    # disable manual
    documentation.nixos.enable = false;

    # disable xterm
    services.xserver.excludePackages = [ pkgs.xterm ];

    # tailscale
    services.tailscale = {
        enable = true;
        useRoutingFeatures = "server";
    };

    # headscale
    services.headscale = {
        enable = true;
        address = "0.0.0.0";
        port = 3001;
        user = "lenni";
        settings = {
            server_url = "https://vpn.bes.lostinthe.cloud";
            dns = {
                nameservers.global = ["100.64.0.2"];
                override_local_dns = true;
                magic_dns = false;
            };
        };
    };

    # Enable the OpenSSH daemon.
    services.openssh = {
        enable = true;
        settings = {
            X11Forwarding = true;
            GatewayPorts = "yes";
            PasswordAuthentication = false;
            # PermitRootLogin = "no";
            KbdInteractiveAuthentication = false;
        };
    };

    # gitea
    services.gitea = {
        enable = true;
        settings.server = {
            DOMAIN = "git.lennihein.com";
            HTTP_PORT = 3002;
        };
    };

    # services.hedgedoc = {
    #     enable = true;
    #     settings = {
    #         domain = "md.lennihein.com";
    #         port = 3003;
    #         protocolUseSSL = true;
    #         services.gitea.settings.server.ROOT_URL = "https://md.lennihein.com";
    #     };
    # };

    # AdGuard Home
    services.adguardhome.enable = true;

    # AoE2 Scout
    services.aoe2-scout = {
        enable = true;
        port = 5000;
    };

    # Caddy
    services.caddy = {
        enable = true;
        extraConfig = ''
            vpn.bes.lostinthe.cloud {
                reverse_proxy localhost:3001
            }

            adguard.bes.lostinthe.cloud {
                reverse_proxy http://127.0.0.1:3000
            }

            dns.lostinthe.cloud {
                respond "try TLS or QUICK..."
            }

            git.lennihein.com {
                reverse_proxy localhost:3002
            }

            aoe2.bes.lostinthe.cloud {
                reverse_proxy localhost:5000
            }

            # md.lennihein.com {
            #     reverse_proxy localhost:3003
            # }
        '';
    };

    # disable
    networking.firewall = {
        enable = true;
        trustedInterfaces = [ "tailscale0" ];
        allowedTCPPorts = [ 853 443 80 ];
        allowedUDPPorts = [ 853 ];
    };
    # for tailscale
    networking.nftables.enable = true;

    # automatic upgrades
    # system.autoUpgrade.enable = true;
    # for unmanaged server enable this:
    # system.autoUpgrade.allowReboot = true;
    # every day at 4am
    # system.autoUpgrade.dates = "*-*-* 4:00:00";

    # Dedupe the Nix store
    nix.settings.auto-optimise-store = true;

    # garbage collection
    # nix.gc.automatic = true;
    # nix.gc.dates = "*-*-* 4:00:00";
    # without options it will only clean the store, not delete old generations
    # nix.gc.options = "--delete-older-than 14d";

    # NixOS version
    system.stateVersion = "23.11";
}