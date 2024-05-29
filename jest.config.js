export default {
    preset: 'ts-jest/presets/default-esm',
    testEnvironment: 'node',
    globals: {
        'ts-jest': {
            diagnostics: false,
            tsconfig: './tsconfig.json',
            isolatedModules: true,
        },
    },
};
