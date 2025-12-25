{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  packages = [
    # Python with HTTP + HTML parsing and notebook kernel
    (pkgs.python3.withPackages (ps: with ps; [ requests beautifulsoup4 lxml ipykernel ]))
    pkgs.jupyter
  ];

  shellHook = ''
    echo "AoE2 notebook env ready. Launch with: jupyter notebook aoe2_match_history.ipynb"
  '';
}
