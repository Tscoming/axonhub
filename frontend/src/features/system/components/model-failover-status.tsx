'use client';

import { Loader2, RefreshCw } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { useModelFailoverStatuses } from '../data/system';

function formatTime(value: string | undefined, locale: string) {
  return value ? new Date(value).toLocaleString(locale) : '-';
}

export function ModelFailoverStatus() {
  const { t, i18n } = useTranslation();
  const { data = [], isLoading, isFetching, refetch } = useModelFailoverStatuses();

  return (
    <div className='space-y-3 rounded-md border p-4'>
      <div className='flex items-start justify-between gap-4'>
        <div>
          <div className='font-medium'>{t('system.retry.modelFailover.status.title')}</div>
          <div className='text-muted-foreground text-sm'>{t('system.retry.modelFailover.status.description')}</div>
        </div>
        <Button type='button' variant='outline' size='sm' disabled={isFetching} onClick={() => refetch()}>
          <RefreshCw className={`mr-2 h-4 w-4 ${isFetching ? 'animate-spin' : ''}`} />
          {t('system.retry.modelFailover.status.refresh')}
        </Button>
      </div>

      {isLoading ? (
        <div className='flex items-center justify-center py-6'>
          <Loader2 className='h-5 w-5 animate-spin' />
        </div>
      ) : data.length === 0 ? (
        <div className='text-muted-foreground bg-muted/30 rounded-md p-4 text-sm'>{t('system.retry.modelFailover.status.empty')}</div>
      ) : (
        <div className='overflow-x-auto'>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>{t('system.retry.modelFailover.status.channel')}</TableHead>
                <TableHead>{t('system.retry.modelFailover.status.model')}</TableHead>
                <TableHead>{t('system.retry.modelFailover.status.state')}</TableHead>
                <TableHead>{t('system.retry.modelFailover.status.failures')}</TableHead>
                <TableHead>{t('system.retry.modelFailover.status.lastFailure')}</TableHead>
                <TableHead>{t('system.retry.modelFailover.status.lastSuccess')}</TableHead>
                <TableHead>{t('system.retry.modelFailover.status.nextProbe')}</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data.map((status) => (
                <TableRow key={`${status.channelID}:${status.modelID}`}>
                  <TableCell>
                    <div className='font-medium'>{status.channelName || `#${status.channelID}`}</div>
                    <div className='text-muted-foreground text-xs'>#{status.channelID}</div>
                  </TableCell>
                  <TableCell className='font-mono text-sm'>{status.modelID}</TableCell>
                  <TableCell>
                    <Badge variant={status.state === 'open' ? 'destructive' : 'secondary'}>
                      {t(`system.retry.modelFailover.status.states.${status.state}`)}
                    </Badge>
                  </TableCell>
                  <TableCell>{status.consecutiveFailures}</TableCell>
                  <TableCell className='text-sm whitespace-nowrap'>{formatTime(status.lastFailureAt, i18n.language)}</TableCell>
                  <TableCell className='text-sm whitespace-nowrap'>{formatTime(status.lastSuccessAt, i18n.language)}</TableCell>
                  <TableCell className='text-sm whitespace-nowrap'>{formatTime(status.nextProbeAt, i18n.language)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  );
}
