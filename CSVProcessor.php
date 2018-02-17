<?php

class CSVProcessor {

    private $pdo;
    private $dbName;

    private $regex = [
        "eventDatetime"     => "/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/",
        "eventAction"       => "/[a-z]{1,20}$/",
        "callRef"           => "/^\d+$/",
        "eventValue"        => "/^\d*\.\d*$/",
    ];

    /**
     * CSVProcessor constructor
     *
     * @param PDO $pdo
     * @param $dbName
     */
    public function __construct(\PDO $pdo, $dbName)
    {
        $this->pdo = $pdo;
        $this->dbName = $dbName;
        $this->setupDatabase();
        openlog('CSV PROCESSOR', LOG_PERROR | LOG_PID, LOG_LOCAL0);
    }

    /**
     * Processes files if no LOCKFILE exists and CSV files are available
     */
    public function process()
    {
        if (file_exists(__DIR__ . '/LOCKFILE')) {
            syslog(LOG_INFO, 'Process already running - LOCKFILE found');
            exit;
        }

        // Find CSV files and process, creating output directory if necessary
        if (is_dir(__DIR__ . '/uploaded')) {
            $files = glob(__DIR__ . '/uploaded/*.csv');
            if (count($files) == 0) {
                syslog(LOG_INFO, 'No files available for processing');
                exit;
            }

            $processed_dir = __DIR__ . DIRECTORY_SEPARATOR . 'processed';
            if (!is_dir($processed_dir)) {
                mkdir($processed_dir);
            }

            $this->processFiles($files);
        }
    }

    /**
     * Processes files one-by-one and moves once completed
     *
     * @param $files
     */
    private function processFiles($files)
    {
        file_put_contents(__DIR__ . DIRECTORY_SEPARATOR . 'LOCKFILE', 'LOCKED');
        foreach($files as $file) {
            syslog(LOG_INFO, sprintf("Processing %s", $file));
            $handle = fopen($file, 'r');
            $headers = fgetcsv($handle);

            // iterate each row of CSV
            while ($row = fgetcsv($handle)) {
                // skip bad rows
                if (count($row) !== 5) {
                    syslog(LOG_DEBUG, 'Bad or empty row found');
                    continue;
                }

                // creates associative array with keys and checks validity before saving to DB
                $rowData = array_combine($headers, $row);
                if ($this->isValidRow($rowData)) {
                    $this->saveRow($rowData);
                }
            }

            fclose($handle);

            $file_path = explode(DIRECTORY_SEPARATOR, $file);
            $filename = array_pop($file_path);
            $new_filename = __DIR__ . DIRECTORY_SEPARATOR . 'processed' . DIRECTORY_SEPARATOR . $filename;

            rename($file, $new_filename);
        }

        // Remove lock file when completed
        unlink(__DIR__ . DIRECTORY_SEPARATOR . 'LOCKFILE');
    }

    /**
     * Checks row data validity using individual regex checkers for each field type
     *
     * @param $rowData
     *
     * @return bool
     */
    private function isValidRow($rowData)
    {
        $valid = true;

        foreach(['eventDatetime', 'eventAction', 'callRef'] as $required_field) {
            if (!$this->isValidField($required_field, $rowData[$required_field])) {
                $valid = false;
            }
        }

        if (!empty($rowData['eventValue'])) {
            if (!$this->isValidEventValue($rowData['eventValue'])) {
                $valid = false;
            }
            if (!$this->isValidEventCurrencyCode($rowData['eventCurrencyCode'])) {
                $valid = false;
            }
        }

        if (!$valid) {
            syslog(LOG_DEBUG, 'Invalid row data');
        }

        return $valid;
    }

    /**
     * Saves a row to the DB with statement
     *
     * @param $rowData
     */
    private function saveRow($rowData)
    {
        $sql = "
            INSERT INTO event (eventDatetime, eventAction, callRef, eventValue, eventCurrencyCode)
            VALUES (?, ?, ?, ?, ?);
        ";

        $stmt = $this->pdo->prepare($sql);
        $stmt->execute([
            $rowData['eventDatetime'],
            $rowData['eventAction'],
            $rowData['callRef'],
            $rowData['eventValue'],
            $rowData['eventCurrencyCode'],
        ]);
    }

    /**
     * Create DB and table if not existing already
     */
    private function setupDatabase()
    {
        $sql = "
            CREATE TABLE IF NOT EXISTS event (
                eventDatetime DATETIME NOT NULL,
                eventAction VARCHAR(20) NOT NULL,
                callRef INT(4) NOT NULL,
                eventValue DECIMAL(10,2),
                eventCurrencyCode VARCHAR(3)
            );
        ";

        $this->pdo->exec("CREATE DATABASE IF NOT EXISTS {$this->dbName}");
        $this->pdo->exec("USE {$this->dbName}");
        $this->pdo->exec($sql);
    }

    /**
     * Proxy method to check if a field is valid
     *
     * @param $field
     * @param $value
     *
     * @return bool
     */
    private function isValidField($field, $value)
    {
        $field = ucfirst($field);
        $method = sprintf("isValid%s", $field);

        if (method_exists($this, $method)) {
            return $this->$method($value);
        }
    }


    private function isValidEventDatetime($value)
    {
        return preg_match($this->regex['eventDatetime'], $value) === 1;
    }

    private function isValidEventAction($value)
    {
        return preg_match($this->regex['eventAction'], $value) === 1;
    }

    private function isValidCallRef($value)
    {
        return preg_match($this->regex['callRef'], $value) === 1;
    }

    private function isValidEventValue($value)
    {
        return preg_match($this->regex['eventValue'], $value) === 1;
    }

    private function isValidEventCurrencyCode($value)
    {
        $validCodes = [
            'AED',
            'AFN',
            'ALL',
            'AMD',
            'ANG',
            'AOA',
            'ARS',
            'AUD',
            'AWG',
            'AZN',
            'BAM',
            'BBD',
            'BDT',
            'BGN',
            'BHD',
            'BIF',
            'BMD',
            'BND',
            'BOB',
            'BRL',
            'BSD',
            'BTN',
            'BWP',
            'BYN',
            'BZD',
            'CAD',
            'CDF',
            'CHF',
            'CLP',
            'CNY',
            'COP',
            'CRC',
            'CUC',
            'CUP',
            'CVE',
            'CZK',
            'DJF',
            'DKK',
            'DOP',
            'DZD',
            'EGP',
            'ERN',
            'ETB',
            'EUR',
            'FJD',
            'FKP',
            'GBP',
            'GEL',
            'GGP',
            'GHS',
            'GIP',
            'GMD',
            'GNF',
            'GTQ',
            'GYD',
            'HKD',
            'HNL',
            'HRK',
            'HTG',
            'HUF',
            'IDR',
            'ILS',
            'IMP',
            'INR',
            'IQD',
            'IRR',
            'ISK',
            'JEP',
            'JMD',
            'JOD',
            'JPY',
            'KES',
            'KGS',
            'KHR',
            'KMF',
            'KPW',
            'KRW',
            'KWD',
            'KYD',
            'KZT',
            'LAK',
            'LBP',
            'LKR',
            'LRD',
            'LSL',
            'LYD',
            'MAD',
            'MDL',
            'MGA',
            'MKD',
            'MMK',
            'MNT',
            'MOP',
            'MRU',
            'MUR',
            'MVR',
            'MWK',
            'MXN',
            'MYR',
            'MZN',
            'NAD',
            'NGN',
            'NIO',
            'NOK',
            'NPR',
            'NZD',
            'OMR',
            'PAB',
            'PEN',
            'PGK',
            'PHP',
            'PKR',
            'PLN',
            'PYG',
            'QAR',
            'RON',
            'RSD',
            'RUB',
            'RWF',
            'SAR',
            'SBD',
            'SCR',
            'SDG',
            'SEK',
            'SGD',
            'SHP',
            'SLL',
            'SOS',
            'SPL',
            'SRD',
            'STN',
            'SVC',
            'SYP',
            'SZL',
            'THB',
            'TJS',
            'TMT',
            'TND',
            'TOP',
            'TRY',
            'TTD',
            'TVD',
            'TWD',
            'TZS',
            'UAH',
            'UGX',
            'USD',
            'UYU',
            'UZS',
            'VEF',
            'VND',
            'VUV',
            'WST',
            'XAF',
            'XCD',
            'XDR',
            'XOF',
            'XPF',
            'YER',
            'ZAR',
            'ZMW',
            'ZWD'
        ];

        return in_array($value, $validCodes);
    }

}