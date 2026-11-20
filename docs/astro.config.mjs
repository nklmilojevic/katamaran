import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLinksValidator from 'starlight-links-validator';

export default defineConfig({
	site: 'https://nklmilojevic.github.io',
	base: '/katamaran',
	trailingSlash: 'always',
	integrations: [
		starlight({
			title: 'katamaran',
			description: 'Fast Docker containers and Linux machines on Apple silicon Macs.',
			logo: { src: './src/assets/icon.png', alt: '' },
			favicon: '/favicon.png',
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/nklmilojevic/katamaran' },
			],
			editLink: {
				baseUrl: 'https://github.com/nklmilojevic/katamaran/edit/main/docs/',
			},
			customCss: [
				'@fontsource-variable/newsreader',
				'@fontsource-variable/inter',
				'@fontsource/ibm-plex-mono/400.css',
				'@fontsource/ibm-plex-mono/500.css',
				'./src/styles/custom.css',
			],
			expressiveCode: {
				themes: ['vitesse-light', 'vitesse-dark'],
				styleOverrides: {
					borderRadius: '0.375rem',
					codeFontFamily: "'IBM Plex Mono', ui-monospace, SFMono-Regular, monospace",
					codeFontSize: '0.85rem',
					uiFontFamily: "'Inter Variable', ui-sans-serif, system-ui, sans-serif",
					frames: {
						frameBoxShadowCssValue: 'none',
					},
				},
			},
			sidebar: [
				{
					label: 'Get started',
					items: [
						{ label: 'Overview', slug: 'index' },
						'install',
						'quickstart',
					],
				},
				{
					label: 'Guides',
					items: [
						'docker',
						'file-sharing',
						'networking',
						'machines',
						'amd64',
						'app',
						'build-from-source',
					],
				},
				{
					label: 'Reference',
					items: [
						'cli',
						'configuration',
						'benchmarks',
					],
				},
				{
					label: 'Explanation and help',
					items: [
						'architecture',
						'troubleshooting',
					],
				},
			],
			plugins: [
				starlightLinksValidator(),
			],
		}),
	],
});
