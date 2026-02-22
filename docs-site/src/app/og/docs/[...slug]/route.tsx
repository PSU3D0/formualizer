import { getPageImage, source } from '@/lib/source';
import { notFound } from 'next/navigation';
import { ImageResponse } from 'next/og';

export const revalidate = false;

export async function GET(_req: Request, { params }: RouteContext<'/og/docs/[...slug]'>) {
  const { slug } = await params;
  const page = source.getPage(slug.slice(0, -1));
  if (!page) notFound();

  return new ImageResponse(
    (
      <div
        style={{
          height: '100%',
          width: '100%',
          display: 'flex',
          flexDirection: 'column',
          justifyContent: 'center',
          padding: '64px',
          background: '#0b1020',
          color: '#f8fafc',
          fontFamily: 'Inter, sans-serif',
        }}
      >
        <div style={{ fontSize: 24, opacity: 0.7, marginBottom: 16 }}>Formualizer Docs</div>
        <div style={{ fontSize: 56, fontWeight: 700, lineHeight: 1.1, marginBottom: 20 }}>
          {page.data.title}
        </div>
        <div style={{ fontSize: 28, opacity: 0.9, maxWidth: 980 }}>{page.data.description}</div>
      </div>
    ),
    {
      width: 1200,
      height: 630,
    },
  );
}

export function generateStaticParams() {
  return source.getPages().map((page) => ({
    lang: page.locale,
    slug: getPageImage(page).segments,
  }));
}
