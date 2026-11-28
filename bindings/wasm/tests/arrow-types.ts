import { tableFromArrays as apacheTable } from 'apache-arrow';
import { tableFromArrays as flechetteTable } from '@uwdata/flechette';
import { arrowColumns } from 'formualizer/arrow';

arrowColumns(apacheTable({ amount: [1, 2] }));
arrowColumns(flechetteTable({ amount: [1, 2] }));
