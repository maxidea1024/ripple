#!/usr/bin/env ts-node
/**
 * Deploy @gatrix/ripple to game server
 *
 * Usage:
 *   yarn deploy:game                      # Build, pack, and deploy to game server
 *   yarn deploy:game --bump               # Bump patch version before deploy
 *   yarn deploy:game --bump 1.2.3         # Set specific version before deploy
 *   yarn deploy:game --path /path/to/server  # Deploy to custom game server path
 *
 * Environment Variables:
 *   GAME_SERVER_PATH   # Default game server path (default: c:/work/uwo/game/server/node)
 */

import { execSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';

const DEFAULT_GAME_SERVER_PATH = 'c:/work/uwo/game/server/node';
const PACKAGE_SCOPE = '@gatrix';
const PACKAGE_NAME = 'ripple';

interface CliOptions {
  bump: boolean;
  version?: string;
  gameServerPath: string;
}

function parseArgs(): CliOptions {
  const args = process.argv.slice(2);
  const options: CliOptions = {
    bump: false,
    version: undefined,
    gameServerPath: process.env.GAME_SERVER_PATH || DEFAULT_GAME_SERVER_PATH,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];

    if (arg === '--bump') {
      options.bump = true;
      const nextArg = args[i + 1];
      if (
        nextArg &&
        !nextArg.startsWith('--') &&
        /^\d+\.\d+\.\d+/.test(nextArg)
      ) {
        options.version = nextArg;
        i++;
      }
    } else if (arg === '--path') {
      const nextArg = args[i + 1];
      if (nextArg && !nextArg.startsWith('--')) {
        options.gameServerPath = nextArg;
        i++;
      } else {
        console.error('[ERROR] --path requires a path argument');
        process.exit(1);
      }
    }
  }

  return options;
}

function run(cmd: string, _options?: { cwd?: string }): void {
  console.log(`\n> ${cmd}`);
  execSync(cmd, {
    encoding: 'utf-8',
    stdio: 'inherit',
    cwd: _options?.cwd,
  });
}

async function main() {
  const options = parseArgs();
  const gameServerPath = options.gameServerPath;
  const gameServerLibPath = path.join(gameServerPath, 'lib');

  const pkgRoot = path.resolve(__dirname, '..');
  process.chdir(pkgRoot);

  console.log('='.repeat(60));
  console.log('[ripple] Deploying @gatrix/ripple to Game Server');
  console.log('='.repeat(60));
  console.log(`   Target: ${gameServerPath}`);

  // 1. Bump version if requested
  if (options.bump) {
    if (options.version) {
      console.log(`\n[version] Setting version to ${options.version}...`);
      const pkgJson = JSON.parse(fs.readFileSync('package.json', 'utf-8'));
      pkgJson.version = options.version;
      fs.writeFileSync('package.json', JSON.stringify(pkgJson, null, 2) + '\n');
      console.log(`   OK: Version set to ${options.version}`);
    } else {
      console.log('\n[version] Bumping patch version...');
      const pkgJson = JSON.parse(fs.readFileSync('package.json', 'utf-8'));
      const parts = pkgJson.version.split('.').map(Number);
      parts[2]++;
      pkgJson.version = parts.join('.');
      fs.writeFileSync('package.json', JSON.stringify(pkgJson, null, 2) + '\n');
      console.log(`   OK: Version bumped to ${pkgJson.version}`);
    }
  }

  // 2. Get current version
  const packageJson = JSON.parse(fs.readFileSync('package.json', 'utf-8'));
  const version = packageJson.version;
  console.log(`\n[info] Current version: ${version}`);

  // 3. Build
  console.log('\n[build] Building...');
  run('yarn build');

  // 4. Pack
  console.log('\n[pack] Packing...');
  const tgzFileName = `gatrix-${PACKAGE_NAME}-${version}.tgz`;
  run('npm pack --pack-destination .');
  if (!fs.existsSync(tgzFileName)) {
    console.error(`[ERROR] Pack file not found: ${tgzFileName}`);
    process.exit(1);
  }

  // 5. Check game server path exists
  if (!fs.existsSync(gameServerPath)) {
    console.error(`[ERROR] Game server path not found: ${gameServerPath}`);
    process.exit(1);
  }

  // Create lib folder if not exists
  if (!fs.existsSync(gameServerLibPath)) {
    console.log(`\n[dir] Creating lib folder: ${gameServerLibPath}`);
    fs.mkdirSync(gameServerLibPath, { recursive: true });
  }

  // 6. Copy to game server
  console.log(`\n[copy] Copying to game server: ${gameServerLibPath}`);
  const destPath = path.join(gameServerLibPath, tgzFileName);
  fs.copyFileSync(tgzFileName, destPath);
  console.log(`   OK: Copied: ${tgzFileName}`);

  // 7. Update game server package.json
  const gamePackageJsonPath = path.join(gameServerPath, 'package.json');
  if (fs.existsSync(gamePackageJsonPath)) {
    console.log('\n[update] Updating game server package.json...');
    const gamePackageJson = JSON.parse(
      fs.readFileSync(gamePackageJsonPath, 'utf-8'),
    );

    if (gamePackageJson.dependencies) {
      const depKey = `${PACKAGE_SCOPE}/${PACKAGE_NAME}`;
      const oldDep = gamePackageJson.dependencies[depKey];
      const newDep = `file:./lib/${tgzFileName}`;
      gamePackageJson.dependencies[depKey] = newDep;

      console.log(`   OK: ${depKey}: ${oldDep || '(new)'} -> ${newDep}`);

      fs.writeFileSync(
        gamePackageJsonPath,
        JSON.stringify(gamePackageJson, null, 2) + '\n',
      );
    }
  }

  // 8. Clean up local tgz
  console.log('\n[clean] Cleaning up...');
  fs.unlinkSync(tgzFileName);
  console.log(`   OK: Removed: ${tgzFileName}`);

  console.log('\n' + '='.repeat(60));
  console.log(`[DONE] @gatrix/ripple v${version} deployed to game server!`);
  console.log('='.repeat(60));
  console.log('\nNext steps:');
  console.log(`  1. cd ${gameServerPath}`);
  console.log('  2. yarn install');
  console.log('  3. yarn build');
}

main().catch((err) => {
  console.error('[ERROR] Deploy failed:', err);
  process.exit(1);
});
