# Home Manager module for k9rs.
#
# Exposed by the flake as `homeModules.default` (and `homeModules.k9rs`).
# `self` is the flake's own outputs, used to default `package` to the build
# from this very flake/revision.
#
# Usage in a Home Manager configuration:
#
#   imports = [ inputs.k9rs.homeModules.default ];
#   programs.k9rs = {
#     enable = true;
#     settings.ui.skin = "dracula";
#   };
self:
{ config, lib, pkgs, ... }:

let
  cfg = config.programs.k9rs;
  inherit (lib) mkEnableOption mkOption mkIf types literalExpression;

  yamlFormat = pkgs.formats.yaml { };

  # A skin/overlay value may be either a path to an existing YAML file or an
  # attrset that we render to YAML ourselves.
  fileType = types.either types.path yamlFormat.type;
  toFile = name: value:
    if lib.isPath value || lib.isString value
    then value
    else yamlFormat.generate name value;
in
{
  options.programs.k9rs = {
    enable = mkEnableOption "k9rs, a Kubernetes TUI written in Rust";

    package = mkOption {
      type = types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      defaultText = literalExpression "k9rs.packages.\${system}.default";
      description = "The k9rs package to use.";
    };

    settings = mkOption {
      type = yamlFormat.type;
      default = { };
      example = literalExpression ''
        {
          ui.skin = "dracula";
          ui.logs.maxLines = 100000;
          daemon.watcherPageSize = 1000;
        }
      '';
      description = ''
        Configuration written to {file}`$XDG_CONFIG_HOME/k9rs/config.yaml`,
        nested under the top-level `k9rs:` key. k9rs validates strictly
        (`deny_unknown_fields`), so typos in keys will be rejected at runtime.
        See the k9rs README for the full schema.
      '';
    };

    skins = mkOption {
      type = types.attrsOf fileType;
      default = { };
      example = literalExpression ''{ dracula = ./dracula.yaml; }'';
      description = ''
        k9s-compatible skin files. Each entry `name` is written to
        {file}`$XDG_CONFIG_HOME/k9rs/skins/name.yaml` and can be selected via
        {option}`programs.k9rs.settings.ui.skin`. The value is either a path to
        an existing YAML file or an attrset rendered to YAML.
      '';
    };

    overlays = mkOption {
      type = types.attrsOf fileType;
      default = { };
      example = literalExpression ''{ nodeclaims = ./nodeclaims.yaml; }'';
      description = ''
        Resource overlay files (extra columns, coloring rules, key bindings).
        Each entry `name` is written to
        {file}`$XDG_CONFIG_HOME/k9rs/overlays/name.yaml`.
      '';
    };

    daemon = {
      enable = mkOption {
        type = types.bool;
        default = pkgs.stdenv.hostPlatform.isLinux;
        defaultText = literalExpression "pkgs.stdenv.hostPlatform.isLinux";
        description = ''
          Run the k9rs cache daemon as a background service: a systemd user
          service on Linux, a launchd agent on Darwin. The daemon holds the
          Kubernetes API connections and watch streams so the TUI starts
          instantly and survives restarts. When disabled, run k9rs with
          `--no-daemon` (it then runs the server in-process).
        '';
      };

      path = mkOption {
        type = types.listOf types.package;
        default = [ pkgs.kubectl ];
        defaultText = literalExpression "[ pkgs.kubectl ]";
        example = literalExpression "[ pkgs.kubectl pkgs.awscli2 ]";
        description = ''
          Packages placed on the daemon service's `PATH`. k9rs shells out to
          `kubectl` for describe, exec, port-forward and exec-resource polling,
          so `kubectl` is included by default. If your kubeconfig uses an exec
          credential plugin (e.g. `aws`, `gke-gcloud-auth-plugin`), add the
          providing package here so the daemon can authenticate.
        '';
      };
    };
  };

  config = mkIf cfg.enable {
    home.packages = [ cfg.package ];

    xdg.configFile = lib.mkMerge [
      (mkIf (cfg.settings != { }) {
        "k9rs/config.yaml".source =
          yamlFormat.generate "k9rs-config.yaml" { k9rs = cfg.settings; };
      })
      (lib.mapAttrs'
        (name: value:
          lib.nameValuePair "k9rs/skins/${name}.yaml" {
            source = toFile "k9rs-skin-${name}.yaml" value;
          })
        cfg.skins)
      (lib.mapAttrs'
        (name: value:
          lib.nameValuePair "k9rs/overlays/${name}.yaml" {
            source = toFile "k9rs-overlay-${name}.yaml" value;
          })
        cfg.overlays)
    ];

    systemd.user.services.k9rs-daemon =
      mkIf (cfg.daemon.enable && pkgs.stdenv.hostPlatform.isLinux) {
        Unit = {
          Description = "k9rs Kubernetes TUI cache daemon";
          Documentation = "https://github.com/rt-kill/k9rs";
        };
        Service = {
          ExecStart = "${cfg.package}/bin/k9rs daemon";
          Environment = [ "PATH=${lib.makeBinPath cfg.daemon.path}" ];
          Restart = "on-failure";
          RestartSec = 2;
        };
        Install.WantedBy = [ "default.target" ];
      };

    launchd.agents.k9rs-daemon =
      mkIf (cfg.daemon.enable && pkgs.stdenv.hostPlatform.isDarwin) {
        enable = true;
        config = {
          ProgramArguments = [ "${cfg.package}/bin/k9rs" "daemon" ];
          EnvironmentVariables.PATH = lib.makeBinPath cfg.daemon.path;
          KeepAlive = true;
          RunAtLoad = true;
        };
      };
  };
}
