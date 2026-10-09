import type { Config } from '@jest/types';
// Sync object
const config: Config.InitialOptions = {
  preset: 'ts-jest',
  reporters: ['default', 'jest-junit'],
  globalSetup: './test/setup/global-setup.mjs',
  globalTeardown: './test/setup/global-teardown.mjs',
  testTimeout: 30000,
};
export default config;
