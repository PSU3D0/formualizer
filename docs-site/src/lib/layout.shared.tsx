import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';

export const gitConfig = {
  user: 'psu3d0',
  repo: 'formualizer',
  branch: 'main',
};

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: (
        <span className="inline-flex items-center gap-2">
          <img
            src="/icon.svg"
            alt=""
            width={24}
            height={24}
            className="h-6 w-6"
          />
          <span>Formualizer Docs</span>
        </span>
      ),
    },
    themeSwitch: {
      enabled: true,
      mode: 'light-dark-system',
    },
    links: [
      {
        type: 'main',
        text: 'GitHub',
        url: `https://github.com/${gitConfig.user}/${gitConfig.repo}`,
      },
    ],
    githubUrl: `https://github.com/${gitConfig.user}/${gitConfig.repo}`,
  };
}
